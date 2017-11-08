package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smcoprocessors.OperationAttributesIdentifiers;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class AbstractClusterTest {
    protected static final Log LOG = LogFactory
            .getLog(AbstractClusterTest.class.getName());

    private Clusters clusters;
    protected SmpcConfiguration config;
    protected TableSchema schema;
    protected Map<String, Map<String, ColType>> qualifierColTypes;
    protected Map<String, Map<String, List<byte[]>>> generatedValues;
    protected List<byte[]> rowIdentifiers;

    @BeforeClass
    public static void initializeRedisContainer() throws IOException {
        RedisUtils.initializeRedisContainer();
    }


    public AbstractClusterTest() throws Exception {

        List<String> resources = getResources();
        LOG.debug("Start Cluster");
        clusters = new Clusters(resources, getNumberOfRegionsServers());
        LOG.debug("Cluster Ready");
        Configuration conf = new Configuration();
        conf.addResource(resources.get(0));
        config = new SmpcConfiguration(conf);
        generatedValues = new HashMap<String, Map<String, List<byte[]>>>();
        qualifierColTypes = new HashMap<String, Map<String, ColType>>();
        rowIdentifiers = new ArrayList<byte[]>();
    }

    private List<Put> generatePuts() {
        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < getNumberOfRecords(); i++) {
            byte[] id = ("" + i).getBytes();
            Put put = new Put(id);

            List<Family> fams = this.schema.getColumnFamilies();

            for (Family fam : fams) {
                for (Qualifier qual : fam.getQualifiers()) {
                    byte[] val = null;

                    switch (getColType(fam.getFamilyName(), qual.getName())) {
                        case STRING:
                            val = ValuesGenerator.randomString(10).getBytes();
                            break;
                        case INT:
                            //val = ValuesGenerator.randomBigInteger(10).toByteArray();

                            boolean invalid = true;
                            do {
                                int intVal = ValuesGenerator.randomInt();
                                val = BigInteger.valueOf(intVal).toByteArray();
                                if (val.length == 4) {
                                    /** Some integer values are small and have a byte array of length 3. Since HBase
                                     * does a lexicographic comparision based on the byte array, if the size of the byte
                                     * array of every value are not the same, the comparision is not fair. HBase will
                                     * not return a correct result because the comparision is based on the bytes and not
                                     * on the numerical value. If the byte array of the number is smaller than three,
                                     * then generate a new one until it is valid.
                                     */
                                    invalid = false;
                                }
                            } while (invalid);
                            break;

                    }
                    put.add(fam.getFamilyName().getBytes(), qual.getName()
                            .getBytes(), val);

                    recordStoredValue(fam.getFamilyName(), qual.getName(), val);
                }
            }
            rowIdentifiers.add(id);
            puts.add(put);
        }
        return puts;
    }

    protected abstract void generateTableSchema();

    protected abstract long getNumberOfRecords();

    protected abstract Filter getFilterOnUnprotectedColumn();

    protected abstract Filter getFilterOnProtectedColumn();

    protected abstract int getNumberOfRegions();

    protected abstract int getNumberOfRegionsServers();

    protected abstract List<String> getResources();

    protected abstract boolean usesMPC();

    @After
    public void tearDown() throws IOException, InterruptedException {
        // Wait for ports to be free for next tests
        clusters.tearDown();
        Thread.sleep(10000);
    }

    private void validateResult(List<Result> vanillaScanResult, List<Result> protectedScanResult) {


        assertEquals(vanillaScanResult.size(), protectedScanResult.size());

        for (int i = 0; i < vanillaScanResult.size(); i++) {
            Result normalResult = vanillaScanResult.get(i);
            Result protectedResult = vanillaScanResult.get(i);
            byte[] normalRow = normalResult.getRow();
            byte[] protectedRow = protectedResult.getRow();
            assertArrayEquals(normalRow, protectedRow);

            for (Family fam : schema.getColumnFamilies()) {
                for (Qualifier qual : fam.getQualifiers()) {
                    byte[] normalVal = normalResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                    byte[] safeVal = protectedResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                    assertArrayEquals(normalVal, safeVal);
                }
            }
        }

    }

    private void recordStoredValue(String cf, String cq, byte[] val) {

        if (!generatedValues.containsKey(cf)) {
            generatedValues.put(cf, new HashMap<String, List<byte[]>>());
        }

        if (!generatedValues.get(cf).containsKey(cq)) {
            generatedValues.get(cf).put(cq, new ArrayList<byte[]>());
        }
        generatedValues.get(cf).get(cq).add(val);
    }

    private ColType getColType(String fam, String qual) {
        return this.qualifierColTypes.get(fam).get(qual);
    }

    protected List<byte[]> getSplitKeys(int nRegions) {
        int nElemsPerRegion = rowIdentifiers.size() / nRegions;
        List<byte[]> res = new ArrayList<byte[]>();

        int i = nElemsPerRegion;
        do {
            res.add(rowIdentifiers.get(i));
            i += nElemsPerRegion;
        } while (i < rowIdentifiers.size());

        return res;
    }


    @Test
    public void test() throws IOException, InterruptedException {
        generateTableSchema();

        List<Family> families = schema.getColumnFamilies();
        List<String> familyNames = new ArrayList<String>();

        for (Family fam : families) {
            familyNames.add(fam.getFamilyName());
        }
        LOG.debug("Generate puts");
        List<Put> puts = generatePuts();
        LOG.debug("Create tables");
        TestClusterTables tables;
        if(getNumberOfRegions() > 1) {
            LOG.debug("Creating table with splits");
            List<byte[]> splitKeys = getSplitKeys(getNumberOfRegions());
            tables = clusters.createTablesByteList(schema.getTablename(),
                    familyNames, splitKeys);
        }else{
            LOG.debug("Create table without splits " + schema.getTablename());
            tables = clusters.createTables(schema.getTablename(), familyNames);
        }

        assert tables != null;
        tables.setSchema(schema);

        if(usesMPC()){
            LOG.debug("Test uses MPC protocols");
            tables.usesMpc();
        }

        LOG.debug("Insert records");
        for (Put p : puts) {
            tables.put(p);
        }
        LOG.debug("Execute test");
        testExecution(tables);
    }


    private List<Result> getVanillaResult(TestClusterTables tables) throws IOException, InterruptedException {
        Scan scan = new Scan();

        scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier, "1".getBytes());
        scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, "1".getBytes());
        Filter filter = getFilterOnUnprotectedColumn();
        scan.setFilter(filter);

        ClusterScanResult clusterScanResult = tables.scan(scan, true);
        return decodeResults(clusterScanResult.getAllResults(), true);
    }

    private List<Result> getProtectedResult(TestClusterTables tables) throws IOException, InterruptedException {
        Scan scan = new Scan();

        scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier, "1".getBytes());
        scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, "1".getBytes());
        scan.setAttribute(OperationAttributesIdentifiers.ScanType.ProtectedColumnScan.name(), "true".getBytes());

        Filter filter = getFilterOnProtectedColumn();
        scan.setFilter(filter);

        ClusterScanResult clusterScanResult = tables.scan(scan, false);
        return decodeResults(clusterScanResult.getAllResults(), false);

    }

    private List<Result> decodeResults(List<List<Result>> results, boolean isResultOfVanilla){

        return results.get(0);
    }
    private void testExecution(TestClusterTables tables) throws IOException,
            InterruptedException {

        List<Result> normalScanResults = getVanillaResult(tables);
        List<Result> protectedScanResult = getProtectedResult(tables);
        validateResult(normalScanResults, protectedScanResult);
    }


    public enum ColType {
        STRING, INT
    }



}
