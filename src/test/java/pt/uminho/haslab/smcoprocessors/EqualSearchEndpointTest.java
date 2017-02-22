package pt.uminho.haslab.smcoprocessors;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.interfaces.SharedSecret;

public class EqualSearchEndpointTest extends AbstractSearchEndpointTest {

	public EqualSearchEndpointTest() throws Exception {
		super();
	}

	public BigInteger getNonStoredValue(List<BigInteger> values) {
		Set<BigInteger> vals = new HashSet<BigInteger>(values);
		BigInteger nVal = values.get(0).add(BigInteger.ONE);
		while (vals.contains(nVal)) {
			nVal.add(BigInteger.ONE);

		}

		return nVal;

	}

	public void searchEndpointComparision(Dealer dealer,
			List<BigInteger> values, TestClusterTables tables, int nbits)
			throws InvalidSecretValue, Throwable {
		LOG.debug("Going to enter searcEndpointComparison");

		/*
		 * For each values stored, go through the list again and secret share
		 * again the value. With the secret shared value do a search operation.
		 * this search operation will return the key of the row that has the
		 * same value. Since each value was stored it must always return the
		 * corrected index. the index of the value position is the same as the
		 * key.
		 */
		long start = System.nanoTime();

		for (int i = 0; i < values.size() - 1; i++) {

			BigInteger value = values.get(i);
			LOG.debug("Going to search for value " + value + " in position "
					+ i);
			SharedSecret secret = dealer.share(value);

			LOG.debug("Going to compare value " + value);
			/*
			 * The computations will be made on 63 bits on reality. Always the
			 * nbits used on the dealer +1. nbits+1. Explanation on the
			 * description of the paramters of the region server and smhbase.
			 */
			LOG.debug("Exepecting row " + i);
			int result = tables.equalEndpoint(nbits + 1, secret, 1, config);

			assertEquals(i, result);
		}

		BigInteger value = getNonStoredValue(values);
		SharedSecret secret = dealer.share(value);
		int result = tables.equalEndpoint(nbits + 1, secret, 1, config);
		assertEquals(-1, result);
		long end = System.nanoTime();
		long duration = TimeUnit.SECONDS.convert(end - start,
				TimeUnit.NANOSECONDS);
		LOG.debug("Execution time was " + duration + " seconds");
	}

	@Override
	protected String getTestTableName() {
		return "EqualTable";
	}

}
