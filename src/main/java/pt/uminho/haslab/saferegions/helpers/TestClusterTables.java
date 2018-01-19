package pt.uminho.haslab.saferegions.helpers;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import pt.uminho.haslab.protocommunication.Smpc;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.OperationAttributesIdentifiers;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.interfaces.Dealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSharedSecret;
import pt.uminho.haslab.smpc.sharemindImp.Integer.IntSharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.Long.LongSharemindDealer;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ClusterTables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestClusterTables extends ClusterTables {

    static final Log LOG = LogFactory.getLog(TestClusterTables.class.getName());
    private boolean usesMPC;
    private TableSchema schema;

    public TestClusterTables(List<Configuration> configs, TableName tbname)
            throws IOException {
        super(configs, tbname);
    }


    public void usesMpc() {
        this.usesMPC = true;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }


    public ClusterTables put(Put put) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

        if (!usesMPC) {
            for (HTable table : tables) {
                table.put(put);
            }
        } else {

            //assuming row identifiers are not protected with secret sharing, just columns

            byte[] identifier = put.getRow();
            Put p1 = new Put(identifier);
            Put p2 = new Put(identifier);
            Put p3 = new Put(identifier);

            for (Family fam : schema.getColumnFamilies()) {
                for (Qualifier qual : fam.getQualifiers()) {
                    byte[] cfb = fam.getFamilyName().getBytes();
                    byte[] cqb = qual.getName().getBytes();
                    List<Cell> cell = put.get(cfb, cqb);


                    byte[] val = CellUtil.cloneValue(cell.get(0));
                    String safeQual = qual.getName();
                    byte[] bSafeQual = safeQual.getBytes();
                    try {

                        switch (qual.getCryptoType()) {

                            case ISMPC:
                                int value = ByteBuffer.wrap(val).getInt();
                                IntSharemindDealer intSharemindDealer = new IntSharemindDealer();
                                int[] secrets = intSharemindDealer.share(value);
                                Put[] puts = new Put[3];
                                puts[0] = p1;
                                puts[1] = p2;
                                puts[2] = p3;

                                for (int i = 0; i < secrets.length; i++) {
                                    int secretValue = secrets[i];
                                    Put p = puts[i];
                                    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                                    byteBuffer.putInt(secretValue);
                                    byteBuffer.flip();
                                    p.add(cfb, bSafeQual, byteBuffer.array());
                                }
                                break;
                            case LSMPC:
                                long lValue = ByteBuffer.wrap(val).getLong();
                                LongSharemindDealer longSharemindDealer = new LongSharemindDealer();
                                long[] lSecrets = longSharemindDealer.share(lValue);
                                Put[] lPuts = new Put[3];
                                lPuts[0] = p1;
                                lPuts[1] = p2;
                                lPuts[2] = p3;

                                //LOG.debug("Inserting value " + lValue + " with secrets " + Arrays.toString(lSecrets)) ;
                                for (int i = 0; i < lSecrets.length; i++) {
                                    long secretValue = lSecrets[i];
                                    Put p = lPuts[i];
                                    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
                                    byteBuffer.putLong(secretValue);
                                    byteBuffer.flip();
                                    p.add(cfb, bSafeQual, byteBuffer.array());
                                }
                                break;

                            case SMPC:
                                BigInteger bigVal = new BigInteger(val);
                                Dealer dealer = new SharemindDealer(qual.getFormatSize());
                                SharemindSharedSecret secret = (SharemindSharedSecret) dealer
                                        .share(bigVal);

                                p1.add(cfb, bSafeQual, secret.getU1().toByteArray());
                                p2.add(cfb, bSafeQual, secret.getU2().toByteArray());
                                p3.add(cfb, bSafeQual, secret.getU3().toByteArray());
                                break;
                            default:
                                p1.add(cfb, cqb, val);
                                p2.add(cfb, cqb, val);
                                p3.add(cfb, cqb, val);

                        }
                        String originalVaLQual = qual.getName() + "_original";
                        byte[] bOriginalValQual = originalVaLQual.getBytes();

                        p1.add(cfb, bOriginalValQual, val);
                        p2.add(cfb, bOriginalValQual, val);
                        p3.add(cfb, bOriginalValQual, val);

                    } catch (InvalidNumberOfBits invalidNumberOfBits) {
                        LOG.debug("Number of bytes is not valid " + qual.getFormatSize());
                        throw new IllegalStateException(invalidNumberOfBits);
                    } catch (InvalidSecretValue invalidSecretValue) {
                        LOG.debug("Invalid secret value");
                        throw new IllegalStateException(invalidSecretValue);
                    }
                }
            }
            tables.get(0).put(p1);
            tables.get(1).put(p2);
            tables.get(2).put(p3);
        }

        return this;
    }

    public ClusterScanResult endpointScans(List<Scan> scans) throws IOException,
            InterruptedException {

        List<EndpointScan> tscans = new ArrayList<EndpointScan>();
        List<List<Result>> results = new ArrayList<List<Result>>();

        for (int i = 0; i < scans.size(); i++) {
            HTable table = tables.get(i);
            Scan scan = scans.get(i);
            LOG.debug("Creating new Concurrent Scan");
            tscans.add(new EndpointScan(table, scan));
        }

        for (EndpointScan t : tscans) {
            LOG.debug("Launching concurrent Scans");
            t.start();
        }

        for (EndpointScan t : tscans) {
            LOG.debug("Joining concurrent scans");
            t.join();

        }

        for (EndpointScan t : tscans) {
            results.add(t.getResults());
        }

        return new ClusterScanResult(results);
    }

    public ClusterScanResult scan(Scan oScan, boolean vanilla) throws IOException, InterruptedException {
        if (vanilla) {
            return super.scan(oScan);
        } else {
            Filter originalFilter = oScan.getFilter();
            List<Filter> parsedFilters = handleFilterWithProtectedColumns(originalFilter);
            List<Scan> scans = plainScans();
            assert parsedFilters != null;

            for (int i = 0; i < scans.size(); i++) {
                Scan scan = scans.get(i);
                scan.setFilter(parsedFilters.get(i));
                scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier, "1".getBytes());
                scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, "1".getBytes());
                scan.setAttribute(OperationAttributesIdentifiers.ScanType.ProtectedColumnScan.name(), "true".getBytes());
            }
            return endpointScans(scans);
        }

    }

    private List<Scan> plainScans() {
        List<Scan> scans = new ArrayList<Scan>();

        Scan scanC1 = new Scan();
        Scan scanC2 = new Scan();
        Scan scanC3 = new Scan();

        scans.add(scanC1);
        scans.add(scanC2);
        scans.add(scanC3);
        return scans;
    }

    private List<Filter> handleFilterWithProtectedColumns(Filter original) {

        if (original instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilter((SingleColumnValueFilter) original);
        } else if (original instanceof FilterList) {
            return handleFilterList((FilterList) original);
        } else if (original instanceof WhileMatchFilter) {
            return handleWhileMatchFilter((WhileMatchFilter) original);
        }
        return null;
    }

    private List<Filter> handleWhileMatchFilter(WhileMatchFilter original) {
        Filter f = original.getFilter();
        List<Filter> handledFilter = handleFilterWithProtectedColumns(f);

        List<Filter> resFilters = new ArrayList<Filter>();

        assert handledFilter != null;
        for (Filter filt : handledFilter) {
            resFilters.add(new WhileMatchFilter(filt));
        }

        return resFilters;
    }

    private List<Filter> handleFilterList(FilterList filter) {
        List<Filter> list = filter.getFilters();
        List<List<Filter>> resultInnerFilters = new ArrayList<List<Filter>>();

        assert list != null;
        for (Filter f : list) {
            resultInnerFilters.add(handleFilterWithProtectedColumns(f));
        }

        List<Filter> results = new ArrayList<Filter>();

        for (int i = 0; i < 3; i++) {
            FilterList fRes = new FilterList(filter.getOperator());

            for (List<Filter> handledFilter : resultInnerFilters) {
                fRes.addFilter(handledFilter.get(i));
            }
            results.add(fRes);
        }
        return results;
    }

    private List<Filter> handleSingleColumnValueFilter(SingleColumnValueFilter filter) {

        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();
        CompareFilter.CompareOp operator = filter.getOperator();
        List<Filter> fList = new ArrayList<Filter>();

        if (DatabaseSchema.isProtectedColumn(schema, family, qualifier)) {
            String sFamily = new String(family);
            String sQualifier = new String(qualifier);

            DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(sFamily, sQualifier);
            byte[] sQualifierMod = sQualifier.getBytes();
            try {
                switch (type) {
                    case SMPC:
                        int formatSize = schema.getFormatSizeFromQualifier(sFamily, sQualifier);
                        Dealer dealer = new SharemindDealer(formatSize);
                        BigInteger bigVal = new BigInteger(value);
                        SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU1().toByteArray()));
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU2().toByteArray()));
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU3().toByteArray()));
                        //LOG.debug("Filter value is encoded in " + secret.getU1() + " : " + secret.getU2() + " : " + secret.getU3());
                        break;
                    case ISMPC:
                        IntSharemindDealer iDealer = new IntSharemindDealer();
                        int iValue = ByteBuffer.wrap(value).getInt();
                        int[] secrets = iDealer.share(iValue);

                        for (int i = 0; i < 3; i++) {
                            int usecret = secrets[i];
                            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                            byteBuffer.putInt(usecret);
                            byteBuffer.flip();
                            fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, byteBuffer.array()));
                        }
                        LOG.debug("Value to search " + iValue + " encoded in  integer secrets " + Arrays.toString(secrets));
                        break;
                    case LSMPC:
                        LongSharemindDealer lDealer = new LongSharemindDealer();
                        long lValue = ByteBuffer.wrap(value).getLong();
                        long[] lSecrets = lDealer.share(lValue);
                        for (int i = 0; i < 3; i++) {
                            long usecret = lSecrets[i];
                            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
                            byteBuffer.putLong(usecret);
                            byteBuffer.flip();
                            fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, byteBuffer.array()));
                        }
                        LOG.debug("Value to search " + lValue + " encoded in  long secrets " + Arrays.toString(lSecrets));
                        break;

                }
            } catch (InvalidNumberOfBits | InvalidSecretValue ex) {
                LOG.debug(ex);
                throw new IllegalStateException(ex);
            }
        } else {
            fList.add(filter);
            fList.add(filter);
            fList.add(filter);
        }

        return fList;
    }

    private class EndpointScan extends Thread {

        private final Scan scan;
        private final HTable table;
        private final List<Result> results;
        private ResultScanner scanner;

        public EndpointScan(HTable table, Scan scan) {
            this.scan = scan;
            this.table = table;
            results = new ArrayList<Result>();
        }

        public List<Result> getResults() {
            return results;
        }

        @Override
        public void run() {
            try {


                byte[] requestID = scan.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
                int targetPlayer = Integer.parseInt(new String(scan.getAttribute(OperationAttributesIdentifiers.TargetPlayer)));
                EndpointCallback callback = new EndpointCallback(scan, requestID, targetPlayer);
                Map<byte[], Smpc.Results> copResults = table.coprocessorService(Smpc.ConcurrentScanService.class, null, null, callback);
                LOG.debug("Scan result size is " + copResults.size());

                for (Smpc.Results res : copResults.values()) {
                    List<Smpc.Row> rows = res.getRowsList();
                    for (Smpc.Row r : rows) {
                        List<Smpc.Cell> cells = r.getCellsList();
                        List<Cell> resCells = new ArrayList<Cell>();
                        for (Smpc.Cell cell : cells) {
                            byte[] row = cell.getRow().toByteArray();
                            byte[] family = cell.getColumnFamily().toByteArray();
                            byte[] qualifier = cell.getColumnQualifier().toByteArray();
                            long timestamp = cell.getTimestamp();
                            byte[] type = cell.getType().toByteArray();
                            byte[] value = cell.getValue().toByteArray();
                            Cell resCell = CellUtil.createCell(row, family, qualifier, timestamp, type[0], value);
                            resCells.add(resCell);
                        }
                        results.add(Result.create(resCells));
                    }
                }


            } catch (Throwable ex) {
                LOG.debug(ex);
                throw new IllegalStateException(ex);
            }

        }

        public class EndpointCallback implements Batch.Call<Smpc.ConcurrentScanService, Smpc.Results> {

            private final Scan scan;
            private byte[] requestID;
            private int targetPlayer;

            public EndpointCallback(Scan scan, byte[] requestID, int targetPlayer) {
                this.scan = scan;
                this.requestID = requestID;
                this.targetPlayer = targetPlayer;

            }

            @Override
            public Smpc.Results call(Smpc.ConcurrentScanService concurrentScanService) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<Smpc.Results> rpcCallback =
                        new BlockingRpcCallback<Smpc.Results>();

                Smpc.ScanMessage message = Smpc.ScanMessage.newBuilder()
                        .setStartRow(ByteString.copyFrom(scan.getStartRow()))
                        .setStopRow(ByteString.copyFrom(scan.getStopRow()))
                        .setFilter(ByteString.copyFrom(scan.getFilter().toByteArray()))
                        .setTargetPlayer(targetPlayer)
                        .setRequestID(ByteString.copyFrom(requestID)).build();

                concurrentScanService.scan(controller, message, rpcCallback);
                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        }
    }
}
