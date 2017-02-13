package pt.uminho.haslab.smcoprocessors;

import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.testingutils.ScanValidator;

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
			throws InvalidNumberOfBits, InvalidSecretValue, Throwable {

		ScanValidator shelper = new ScanValidator(values);

		startKey = getStartKey(shelper);
		stopKey = getStopKey(shelper);

		// System.out.println("going to call scan endpoint with null keys");
		// List<Result> results = tables.scanEndpoint(nbits, null, null, 1,
		// config, dealer);

		// System.out.println("Results sie is " + results.size());

		List<Result> results = tables.scanEndpoint(nbits, startKey, stopKey, 1,
				config, dealer);

		shelper.validateResults(results);

		// Remove two random values , one start key one end key
		// Create multiple tests, with and without keys
		// Create with while match filter
		// create without match filter but with row filter

	}

	@Override
	protected String getTestTableName() {
		return "ScanTable";
	}

}
