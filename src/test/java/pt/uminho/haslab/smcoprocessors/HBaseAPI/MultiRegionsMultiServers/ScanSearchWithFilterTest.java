package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsMultiServers;

import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smcoprocessors.HBaseAPI.AbstractSearchEndpointTest;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.testingutils.ScanValidator;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ScanSearchWithFilterTest  extends AbstractSearchEndpointTest {

    protected byte[] startKey;

    protected byte[] stopKey;

    public ScanSearchWithFilterTest () throws Exception {
        super();
    }

    protected  byte[] getStartKey(ScanValidator validator){
        return validator.generateStartKey();
    }

    protected byte[] getStopKey(ScanValidator validator){
        return null;
        }

    @Override
    public void searchEndpointComparision(Dealer dealer,
                                          List<BigInteger> values, TestClusterTables tables, int nbits)
            throws Throwable {

        ScanValidator shelper = new ScanValidator(values);

        startKey = getStartKey(shelper);
        stopKey = getStopKey(shelper);
        LOG.debug("Start key to be searched is "+ new BigInteger(startKey));
        List<Result> results = tables.scanWithFilter(startKey, stopKey, 1,
                config, dealer, secretFamily, secretQualifier);

        boolean res = shelper.validateResults(results);
        assertEquals(true, res);
    }

    @Override
    protected String getTestTableName() {
        return "ScanTable";
    }

    protected int getNumberOfRegionServers() {
        return 2;
    }

    protected int getNumberOfRegions() {
        return 2;
    }
}