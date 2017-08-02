package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.testingutils.ScanValidator;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class ScanSearchEndpointTest extends AbstractSearchEndpointTest {

    protected byte[] startKey;

    protected byte[] stopKey;

    public ScanSearchEndpointTest() throws Exception {
        super();
    }

    protected abstract byte[] getStartKey(ScanValidator validator);

    protected abstract byte[] getStopKey(ScanValidator validator);

    @Override
    public void searchEndpointComparision(Dealer dealer,
                                          List<BigInteger> values, TestClusterTables tables, int nbits)
            throws Throwable {

        ScanValidator shelper = new ScanValidator(values);

        startKey = getStartKey(shelper);
        stopKey = getStopKey(shelper);

        List<Result> results = tables.scanEndpoint(nbits, startKey, stopKey, 1,
                config, dealer);

        boolean res = shelper.validateResults(results);
        assertEquals(true, res);
    }

    @Override
    protected String getTestTableName() {
        return "ScanTable";
    }

}
