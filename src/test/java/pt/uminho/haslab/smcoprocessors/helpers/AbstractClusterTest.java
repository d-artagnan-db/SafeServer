package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.junit.After;
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
    public static enum ColType {
        STRING, INT
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
                            int intVal = ValuesGenerator.randomInt();
                            System.out.println("generated value " +intVal);
                            val = BigInteger.valueOf(intVal).toByteArray();
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

        // System.out.println("NElemsPerRegions " + nElemsPerRegion);
        int i = nElemsPerRegion;
        do {
            // System.out.println("SplitValue "+i+" is "+ values.get(i));
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
        /*if(getNumberOfClusters() == 1){
            return results.get(0);
        }else if(getNumberOfClusters() == 3 && isResultOfVanilla){
            return results.get(0);
        }else if(getNumberOfClusters() == 3 && !isResultOfVanilla){

            int nRows = results.get(0).size();
            List<Result> decRes = new ArrayList<Result>();

            for(int i = 0; i < nRows; i++){
                List<Cell> cells = new ArrayList<Cell>();

                for(Family fam: schema.getColumnFamilies()){
                    byte[] cfb = fam.getFamilyName().getBytes();
                    for(Qualifier qual: fam.getQualifiers()){
                        byte[] cqb = qual.getName().getBytes();
                        DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(fam.getFamilyName(), qual.getName());

                        if(type == DatabaseSchema.CryptoType.SMPC){

                        }else{
                            Result rowResult = results.get(0).get(1);
                            Cell rcell = rowResult.getColumnCells(cfb, cqb).get(0);
                            cells.add(rcell);
                        }
                    }
                }
            }

        }*/

    }
    private void testExecution(TestClusterTables tables) throws IOException,
            InterruptedException {

        List<Result> normalScanResults = getVanillaResult(tables);
        List<Result> protectedScanResult = getProtectedResult(tables);
        validateResult(normalScanResults, protectedScanResult);
    }


    private void validateResult(List<Result> vanillaScanResult, List<Result> protectedScanResult){

        System.out.println(vanillaScanResult);
        assertEquals(vanillaScanResult.size(), protectedScanResult.size());
        System.out.println("Compared Result size "+ vanillaScanResult.size());
        for(int i =0; i < vanillaScanResult.size(); i++) {
            Result normalResult = vanillaScanResult.get(i);
            Result protectedResult = vanillaScanResult.get(i);
            byte[] normalRow = normalResult.getRow();
            byte[] protectedRow = protectedResult.getRow();
            assertArrayEquals(normalRow, protectedRow);
            //System.out.println("Compared rows "+ new String(normalRow));
            for (Family fam : schema.getColumnFamilies()) {
                for (Qualifier qual : fam.getQualifiers()) {
                    byte[] normalVal = normalResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                    byte[] safeVal = protectedResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                    assertArrayEquals(normalVal, safeVal);
                    //System.out.println("ComparedValues " + new String(normalVal));
                }
            }
        }

    }



}
