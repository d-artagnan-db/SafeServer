package pt.uminho.haslab.saferegions;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.protocommunication.Smpc;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.comunication.SharemindMessageBroker;
import pt.uminho.haslab.saferegions.secretSearch.ContextPlayer;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;
import pt.uminho.haslab.saferegions.secureRegionScanner.SecureRegionScanner;
import pt.uminho.haslab.smpc.helpers.RandomGenerator;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Semaphore;

public class ConcurrentScanCoprocessor extends Smpc.ConcurrentScanService
        implements Coprocessor, CoprocessorService {

    private static final Log LOG = LogFactory.getLog(ConcurrentScanCoprocessor.class
            .getName());

    private final static Semaphore available = new Semaphore(1);

    /* Region Environment */
    private RegionCoprocessorEnvironment env;

    /* Configuration related to the Player and SearchEndpoint */
    private SmpcConfiguration searchConf;

    /* Holds the value if it was the first execution or not. By default is not. */
    private boolean wasFirst;

    /* Broker used to exchange message between local players and relay */
    private MessageBroker broker;

    private Relay relay;

    /* Database schema with the tables that have protected columns. */
    private DatabaseSchema schema;

    /**
     * Check how many times the start method is instantiated
     *
     * @param e
     * @throws IOException
     */

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
        Configuration conf = e.getConfiguration();
        searchConf = new SmpcConfiguration(conf);

        LOG.info("Starting coprocessor "
                + env.getRegion().getRegionNameAsString());

        try {
            available.acquire();

            if (!playerHasStarted()) {
                LOG.info("Starting player configuration");

                wasFirst = true;
                schema = searchConf.getSchema();
                broker = new SharemindMessageBroker();
                relay = searchConf.createRelay(broker);
                relay.bootRelay();

                //init smpc cache of random BigIntegers
                if (searchConf.getPreRandomSize() > 0) {
                    LOG.debug("Generating batch of random integer " + searchConf.getPreRandomSize());
                    RandomGenerator.initBatch(searchConf.getPreRandomNBits(), searchConf.getPreRandomSize());
                    RandomGenerator.initIntBatch(searchConf.getPreRandomSize());
                    RandomGenerator.initLongBatch(searchConf.getPreRandomSize());
                }

                // Wait some time before trying to connect with other region servers
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Player " + searchConf.getPlayerID()
                            + " is waiting for Relay server to start");
                }
                try {
                    waitServerStart();
                } catch (InterruptedException e1) {
                    LOG.error("Relay not booted correctly "
                            + e1.getLocalizedMessage());
                    throw new IllegalStateException(e1);
                }

                initiateSharedResources(searchConf);
                initOutFiles();
                LOG.info("Resources initiated " + searchConf.getPlayerIDasString());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Second start ");
                }
                setupSharedResources(searchConf);
            }
        } catch (InterruptedException e1) {
            LOG.error(e1);
            throw new IllegalStateException(e1);
        } finally {
            available.release();

        }
    }

    private void initOutFiles() throws IOException {

        /**
         * Java uncaught exceptions are sent to stdout or stderr and not caught on log4j.
         * To catch these exceptions we define an output stream on the jvm to redirect the messages to a file.
         * Maybe integrate this with a log4j. How is this not an option of log4j?
         * */

        PrintStream o = null;
        PrintStream er = null;

        if (searchConf.isDevelopment()) {
            o = new PrintStream(new File("/tmp/out.log"));
            er = new PrintStream(new File("/tmp/error.log"));
        } else {
            o = new PrintStream(new File("/var/log/hbase/out.log"));
            er = new PrintStream(new File("/var/log/hbase/error.log"));
        }
        System.setOut(o);
        System.setErr(er);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (wasFirst) {
            if (!searchConf.isDevelopment()) {
                LOG.debug("Stop Coprocessors was issued " + env.getRegion().getRegionNameAsString());
                relay.stopRelay();
            } else {
                RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) e;

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stoping coprocessor " + env.getRegion().getRegionNameAsString());
                }
                /*
                 * In development mode clusters are not concurrent and the stop
				 * requests by default waits for the other players to cancel
				 * their channel. This only happens if every relay stops the
				 * execution concurrently. This way the server socket is forced
				 * to close.
				 */
                relay.forceStopRelay();
                ((RegionCoprocessorEnvironment) e).getSharedData().clear();

            }
        }
    }


    public void setupSharedResources(SmpcConfiguration conf) {
        Map<String, Object> values = (Map<String, Object>) env.getSharedData()
                .get(searchConf.getPlayerIDasString());
        relay = (Relay) values.get(SharedResourcesIdentifiers.RELAY);
        broker = (MessageBroker) values.get(SharedResourcesIdentifiers.BROKER);
        schema = (DatabaseSchema) values.get(SharedResourcesIdentifiers.SCHEMA);
        searchConf = (SmpcConfiguration) values.get(SharedResourcesIdentifiers.CONFIG);
    }

    private void initiateSharedResources(SmpcConfiguration searchConf) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put(SharedResourcesIdentifiers.RELAY, relay);
        values.put(SharedResourcesIdentifiers.BROKER, broker);
        values.put(SharedResourcesIdentifiers.SCHEMA, schema);
        values.put(SharedResourcesIdentifiers.CONFIG, searchConf);
        env.getSharedData().put(searchConf.getPlayerIDasString(), values);
    }

    /**
     * Helping function that checks if the region server player has started the
     * resources required to execute MPC. This is required because a
     * regionServer calls the start() method multiple times.
     */

    private boolean playerHasStarted() {

        return env.getSharedData()
                .containsKey(searchConf.getPlayerIDasString());
    }

    private void waitServerStart() throws InterruptedException {
        LOG.debug("Waiting for signal of Relay Start");
        broker.waitRelayStart();
        LOG.debug("Relay start signal received");
    }

    private Player getPlayer(RequestIdentifier identifier) {
        return new ContextPlayer(relay, identifier,
                this.searchConf.getPlayerID(), broker);
    }

    private RequestIdentifier getRequestIdentifier(OperationWithAttributes op,
                                                   RegionCoprocessorEnvironment env) {

        byte[] requestID = op.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
        byte[] regionID = env.getRegion().getStartKey();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Region Request unique identifier has requestID " + Arrays.toString(requestID) + " and regionID " + Arrays.toString(regionID));
        }

        return new RequestIdentifier(requestID, regionID);
    }


    private RegionScanner secretScanSearchWithFilter(Scan op,
                                                     RegionCoprocessorEnvironment env, String tableName)
            throws IOException {
        validateOperationAttributes(op);


        RequestIdentifier ident = getRequestIdentifier(op, env);
        //LOG.debug("Going to registerRequest");
        relay.registerRequest(ident);
        Player player = getPlayer(ident);
        byte[] startRow = op.getStartRow();
        byte[] stopRow = op.getStopRow();
        byte[] regionStartKey = env.getRegion().getStartKey();
        byte[] regionEndKey = env.getRegion().getEndKey();

        /*if (LOG.isDebugEnabled()) {
            String requestID = Arrays
                    .toString(ident.getRequestID());
            String regionID = Arrays.toString(ident.getRegionID());
            LOG.debug(player.getPlayerID() + " has scan with "
                    + Arrays.toString(startRow) + ", " + Arrays.toString(stopRow)
                    + ", " + op.getFilter() + " and has identifier with identifier " + requestID+":"+regionID);
        }*/

        checkTargetPlayer(player, op);

        TableSchema tSchema = schema.getTableSchema(tableName);
        /*if (LOG.isDebugEnabled()) {
            LOG.debug("Is player targetPlayer " + ((ContextPlayer) player).isTargetPlayer());
            LOG.debug("Returning SecureRegionScanner");
        }*/
        return new SecureRegionScanner(env, player, this.searchConf,
                startRow, stopRow, regionStartKey, regionEndKey, tSchema, op);
    }

    private void validateOperationAttributes(OperationWithAttributes op) {

        byte[] requestID = op.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
        byte[] targetPlayer = op.getAttribute(OperationAttributesIdentifiers.TargetPlayer);

        if (requestID == null) {
            String error = "RequestID not specified";
            LOG.debug(error);
            throw new IllegalStateException(error);
        }

        if (targetPlayer == null) {
            String error = "TargetPlayer not specified";
            LOG.debug(error);
            throw new IllegalStateException(error);
        }

    }

    private void checkTargetPlayer(Player player, OperationWithAttributes op) {
        String targetPlayerS = new String(
                op.getAttribute(OperationAttributesIdentifiers.TargetPlayer));
        int targetPlayer = Integer.parseInt(targetPlayerS);
        ((SharemindPlayer) player).setTargetPlayer(targetPlayer);
    }

    @Override
    public Service getService() {
        return this;
    }

    private Filter parseFilter(byte[] filter, String filterClassName){
        try {
            Class filterClass = Class.forName(filterClassName);
            Method m = filterClass.getDeclaredMethod("parseFrom",  byte[].class);
            Filter result = (Filter) m.invoke(null, filter);
            return result;

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOG.debug(e);
            throw new IllegalStateException(e);
        }
    }
    @Override
    public void scan(RpcController rpcController, Smpc.ScanMessage scanMessage, RpcCallback<Smpc.Results> rpcCallback) {

        try {
            Scan scan = new Scan(scanMessage.getStartRow().toByteArray(), scanMessage.getStopRow().toByteArray());
            scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,  scanMessage.getRequestID().toByteArray());
            scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, (""+scanMessage.getTargetPlayer()).getBytes());
            Filter f = parseFilter(scanMessage.getFilter().toByteArray(), scanMessage.getFilterType());
            //Filter f = SingleColumnValueFilter.parseFrom(scanMessage.getFilter().toByteArray());
            scan.setFilter(f);
            String table = env.getRegion().getTableDesc().getNameAsString();
            RegionScanner scanner = secretScanSearchWithFilter(scan, env, table);

            List<List<Cell>> results = new ArrayList<List<Cell>>();
            List<Cell>  row = new ArrayList<Cell>();
            Smpc.Results.Builder resBuilder = Smpc.Results.newBuilder();

            boolean run;
            do{
                run = scanner.next(row);
                if(!row.isEmpty()){
                    results.add(row);
                }
                row = new ArrayList<Cell>();

            } while(run);

            for(List<Cell> resRow : results){
                Smpc.Row.Builder  rowBuilder  = Smpc.Row.newBuilder();
                for(Cell cell: resRow){
                    Smpc.Cell.Builder cellBuilder = Smpc.Cell.newBuilder();
                    cellBuilder.setColumnFamily(ByteString.copyFrom(CellUtil.cloneFamily(cell)));
                    cellBuilder.setColumnQualifier(ByteString.copyFrom(CellUtil.cloneQualifier(cell)));
                    cellBuilder.setTimestamp(cell.getTimestamp());
                    cellBuilder.setRow(ByteString.copyFrom(CellUtil.cloneRow(cell)));
                    cellBuilder.setValue(ByteString.copyFrom(CellUtil.cloneValue(cell)));
                    byte[] type = new byte[1];
                    type[0] = cell.getTypeByte();
                    cellBuilder.setType(ByteString.copyFrom(type));
                    rowBuilder.addCells(cellBuilder.build());
                }
                resBuilder.addRows(rowBuilder.build());
            }
            rpcCallback.run(resBuilder.build());

        } catch (IOException e) {
            LOG.debug(e);
            throw new IllegalStateException(e);
        }


    }
}
