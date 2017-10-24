package pt.uminho.haslab.smcoprocessors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.Relay;
import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.comunication.SharemindMessageBroker;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smcoprocessors.secretSearch.*;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.Column;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.SecureRegionScanner;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.smcoprocessors.OperationAttributesIdentifiers.ScanType.Normal;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.*;

public class SmpcCoprocessor extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(SmpcCoprocessor.class
            .getName());

    /* Region Environment */
    private RegionCoprocessorEnvironment env;

    /* Configuration related to the Player and SearchEndpoint */
    private SmpcConfiguration searchConf;

    /* Holds the value if it was the first execution or not. By default is not. */
    private boolean wasFirst;

    /* Broker used to exchange message between local players and relay */
    private MessageBroker broker;

    private Relay relay;

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

        if (!playerHasStarted()) {
            wasFirst = true;
            broker = new SharemindMessageBroker();
            relay = searchConf.createRelay(broker);
            relay.bootRelay();
            // Wait some time before trying to connect with other region servers
            LOG.debug("Player " + searchConf.getPlayerID()
                    + " is waiting for Relay server to start");
            try {
                waitServerStart();
            } catch (InterruptedException e1) {
                LOG.error("Relay not booted correctly "
                        + e1.getLocalizedMessage());
                throw new IllegalStateException(e1);
            }

            initiateSharedResources(searchConf);

            if (searchConf.getPreRandomSize() > 0) {
                LOG.debug("Defining "
                        + searchConf.getPreRandomSize()
                        + " as the number of random numbers on the SMPC library");
                SharemindSecretFunctions.initRandomElemes(
                        searchConf.getPreRandomSize(), searchConf.getnBits());
            }
            LOG.info("Resources initiated " + searchConf.getPlayerIDasString());
        } else {
            LOG.debug("Second start " + searchConf.getPlayerIDasString());
            setupSharedResources(searchConf);
        }

    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (wasFirst) {
            if (!searchConf.isDevelopment()) {
                relay.stopRelay();
            } else {
                /*
                 * In development mode clusters are not concurrent and the stop
				 * requests by default waits for the other players to cancel
				 * their channel. This only happens if every relay stops the
				 * execution concurrently. This way the server socket is forced
				 * to close.
				 */
                relay.forceStopRelay();
            }
        }
    }

    public void setupSharedResources(SmpcConfiguration conf) {
        Map<String, Object> values = (Map<String, Object>) env.getSharedData()
                .get(searchConf.getPlayerIDasString());
        relay = (Relay) values.get(SharedResourcesIdentifiers.RELAY);
        broker = (MessageBroker) values.get(SharedResourcesIdentifiers.BROKER);
    }

    private void initiateSharedResources(SmpcConfiguration searchConf) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put(SharedResourcesIdentifiers.RELAY, relay);
        values.put(SharedResourcesIdentifiers.BROKER, broker);
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

    private Column getSearchColumn(OperationWithAttributes op) {

        byte[] secretFamily = op
                .getAttribute(OperationAttributesIdentifiers.SecretFamily);
        byte[] secretQualifier = op
                .getAttribute(OperationAttributesIdentifiers.SecretQualifier);

        if (LOG.isDebugEnabled()) {
            LOG.debug("SecretFamily " + new String(secretFamily));
            LOG.debug("SecretQualifier " + new String(secretQualifier));
        }
        return new Column(secretFamily, secretQualifier);

    }

    private RequestIdentifier getRequestIdentifier(OperationWithAttributes op,
                                                   RegionCoprocessorEnvironment env) {

        byte[] requestID = op
                .getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
        byte[] regionID = env.getRegion().getStartKey();

        if (LOG.isDebugEnabled()) {
            LOG.debug("RequestID " + new String(requestID));
            LOG.debug("RegionID " + new String(regionID));
        }
        return new RequestIdentifier(requestID, regionID);

    }

    private int checkTargetPlayer(Player player, OperationWithAttributes op,
                                  RequestIdentifier ident) {
        String targetPlayerS = new String(
                op.getAttribute(OperationAttributesIdentifiers.TargetPlayer));
        LOG.debug("TargetPlayer " + targetPlayerS);
        int targetPlayer = Integer.parseInt(targetPlayerS);

        if (this.searchConf.getPlayerID() == targetPlayer) {
            LOG.debug("This RegionServer is target player for the request "
                    + ident.hashCode());
            ((SharemindPlayer) player).setTargetPlayer();
        }
        return targetPlayer;
    }

    private RegionScanner secretEqualScanSearch(Scan op,
                                                RegionCoprocessorEnvironment env) throws IOException {
        Column col = getSearchColumn(op);
        RequestIdentifier ident = getRequestIdentifier(op, env);
        relay.registerRequest(ident);
        Player player = getPlayer(ident);
        int nbits = this.searchConf.getnBits();
        int targetPlayer = checkTargetPlayer(player, op, ident);

        List<byte[]> secrets = new ArrayList<byte[]>();
        byte[] secret = op
                .getAttribute(OperationAttributesIdentifiers.ScanForEqualVal);
        secrets.add(secret);
        LOG.debug(player.getPlayerID() + " is creating search condition with "
                + nbits + " nits " + " and the secret "
                + new BigInteger(secret));
        SearchCondition searchCondition = AbstractSearchValue
                .conditionTransformer(Equal, nbits, secrets, targetPlayer);
        LOG.debug(player.getPlayerID() + " created secure RegionScanner");

        return new SecureRegionScanner(searchCondition, env, player,
                this.searchConf, true, col);

    }

    private byte[] getValueFromAttribute(Scan op, String attribute) {

        byte[] row = op.getAttribute(attribute);
        return row == null ? new byte[0] : row;

    }

    private SearchCondition generateScanSearchCondition(byte[] startRow, byte[] stopRow, int nBits, int targetPlayer) {
        SearchCondition startKeySearch = null;
        SearchCondition endKeySearch = null;

        if (startRow.length != 0) {
            List<byte[]> startRows = new ArrayList<byte[]>();
            startRows.add(startRow);

            startKeySearch = AbstractSearchValue.conditionTransformer(
                    GreaterOrEqualThan, nBits, startRows, targetPlayer);
        }

        if (stopRow.length != 0) {
            List<byte[]> stopRows = new ArrayList<byte[]>();
            stopRows.add(stopRow);

            endKeySearch = AbstractSearchValue.conditionTransformer(Less,
                    nBits, stopRows, targetPlayer);
        }

        SearchCondition keySearch = null;

        if (startRow.length != 0 && stopRow.length != 0) {
            keySearch = new ComposedSearchValue(And, startKeySearch,
                    endKeySearch, targetPlayer);
        } else if (startRow.length != 0 && stopRow.length == 0) {
            keySearch = startKeySearch;
        } else if (startRow.length == 0 && stopRow.length != 0) {
            keySearch = endKeySearch;
        } else if (startRow.length == 0 && stopRow.length == 0) {
            keySearch = new NopSearchValue(Nop, targetPlayer);
        }

        return keySearch;
    }

    private RegionScanner secretScanSearch(Scan op, RegionCoprocessorEnvironment env)
            throws IOException, ResultsLengthMismatch,
            ResultsIdentifiersMismatch {
			/* *
			 * The Scan start and stop row keys have to be passed to the
			 *  coprocessor by operation attributes for the client to
			 * issue a full table scan. A full table scan is necessary
			 * to search in every region of a distributed cluster.
			  */
        byte[] startRow = getValueFromAttribute(op, OperationAttributesIdentifiers.ScanStartVal);
        byte[] stopRow = getValueFromAttribute(op, OperationAttributesIdentifiers.ScanStopVal);
        LOG.debug("Start row is " + new BigInteger(startRow));
        LOG.debug("Stop row is " + new BigInteger(stopRow));

        Column col = getSearchColumn(op);
        RequestIdentifier ident = getRequestIdentifier(op, env);
        relay.registerRequest(ident);
        Player player = getPlayer(ident);
        int nbits = this.searchConf.getnBits();
        int targetPlayer = checkTargetPlayer(player, op, ident);

        SearchCondition searchCondition = generateScanSearchCondition(startRow, stopRow, nbits, targetPlayer);

        return new SecureRegionScanner(searchCondition, env, player, this.searchConf, false, col);

    }


    private RegionScanner secretScanSearchWithFilter(Scan op, RegionCoprocessorEnvironment env) throws IOException {
        RequestIdentifier ident = getRequestIdentifier(op, env);
        relay.registerRequest(ident);
        Player player = getPlayer(ident);
        int nbits = this.searchConf.getnBits();
        int targetPlayer = checkTargetPlayer(player, op, ident);
        List<byte[]> secrets = new ArrayList<byte[]>();
        byte[] secret = op
                .getAttribute(OperationAttributesIdentifiers.FilterValue);
        LOG.debug("SearchValue secret is " + new BigInteger(secret));
        secrets.add(secret);

        LOG.debug(player.getPlayerID() + " is creating search condition with "
                + nbits + " nits " + " and the secret "
                + new BigInteger(secret));
        SearchCondition searchCondition = AbstractSearchValue
                .conditionTransformer(Equal, nbits, secrets, targetPlayer);
        LOG.debug(player.getPlayerID() + " created secure RegionScanner");
        /*return new SecureRegionScanner(searchCondition, env, player,
                this.searchConf, true, col);*/
        return null;
    }


    private OperationAttributesIdentifiers.ScanType checkScanType(OperationWithAttributes op) {

        if (op.getAttribute(Normal.name()) != null) {
            return Normal;
        } else if (op.getAttribute(OperationAttributesIdentifiers.ScanType.ProtectedColumnScan.name()) != null) {
            return OperationAttributesIdentifiers.ScanType.ProtectedColumnScan;
        } else if (op.getAttribute(OperationAttributesIdentifiers.ScanType.ProtectedIdentifierGet.name()) != null) {
            return OperationAttributesIdentifiers.ScanType.ProtectedIdentifierGet;
        } else if (op.getAttribute(OperationAttributesIdentifiers.ScanType.ProtectedIdentifierScan.name()) != null) {
            return OperationAttributesIdentifiers.ScanType.ProtectedIdentifierScan;
        } else {
            return null;
        }
    }


    /**
     * An SMPC enabled Cluster has three possibilities for a Scan operation.
     * - The first is a normal scan where no column or row identifier is protected with secret sharing.
     * In this first case no additional processing is required.
     * <p>
     * - The second case is when a table row identifiers are protected with SMPC. In this case, either
     * gets or Scans queries need to do a full table Scan and execute the necessary MPC protocols
     * to return the correct result. The functions secretEqualScanSearch and secretScanSearch handle these
     * two cases.
     * <p>
     * - The third case is where a table identifiers not protected with secret sharing but some
     * columns are. In this case, a scan may not have a filter over the protected columns and thus
     * no additional processing is required. However, if the Scan has a filter on a protected column,
     * the Coprocessor needs to handle this case and return the correct result.
     * <p>
     * There is an enumeration with four types for these cases
     * - ScanType.Normal (firstCase)
     * - ScanType.ProtectedIdentifierGet (Second case , Get Operation)
     * - ScanType.ProtectedIdentifierScan ( Second case, Scan Operation)
     * - ScanType.ProtectedColumnScan (Third case)
     * <p>
     * Currently, the second case is being ignored by the SafeClient as it assumes that identifiers are not protected
     * with secret sharing and only table columns are.
     */

    @Override
    public RegionScanner postScannerOpen(
            final ObserverContext<RegionCoprocessorEnvironment> c,
            final Scan scan, final RegionScanner s) throws IOException {


        OperationAttributesIdentifiers.ScanType scanType = checkScanType(scan);
        // If scanType does not exist or is normal than it returns the original RegionScanner
        if (scanType != null && scanType != Normal) {
            LOG.debug("Protected column ");
            String table = env.getRegion().getTableDesc().getNameAsString();

            if (!table.contains("hbase")) {
                switch (scanType) {
                    case ProtectedIdentifierGet:
                        return secretEqualScanSearch(scan, env);
                    case ProtectedIdentifierScan:
                        try {
                            return secretScanSearch(scan, env);
                        } catch (ResultsLengthMismatch ex) {
                            LOG.error(ex);
                            throw new IllegalStateException(ex);
                        } catch (ResultsIdentifiersMismatch ex) {
                            LOG.error(ex);
                            throw new IllegalStateException(ex);
                        }
                    case ProtectedColumnScan:
                        return secretScanSearchWithFilter(scan, env);
                }

            }
        }

        return s;
    }

}
