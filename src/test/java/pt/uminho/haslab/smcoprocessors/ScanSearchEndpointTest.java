package pt.uminho.haslab.smcoprocessors;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.client.Result;
import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;

public abstract class ScanSearchEndpointTest extends AbstractSearchEndpointTest {

	protected BigInteger startKey;

	protected BigInteger stopKey;

	public ScanSearchEndpointTest() throws Exception {
		super();
	}

	protected void sortValues(List<BigInteger> values) {

		Collections.sort(values, new Comparator<BigInteger>() {

			public int compare(BigInteger value1, BigInteger value2) {
				return value1.compareTo(value2);
			}
		});

	}

	public abstract void setStartKey(List<BigInteger> values);

	public abstract void setStopKey(List<BigInteger> values);

	public List<BigInteger> getKeysInRange(List<BigInteger> values,
			BigInteger startRow, BigInteger endRow) {
		List<BigInteger> rangeValues = new ArrayList<BigInteger>();
		for (int i = 0; i < values.size(); i++) {
			BigInteger val = values.get(i);

			boolean greaterOrEqualThan = val.compareTo(startRow) == 0
					|| val.compareTo(startRow) == 1;
			boolean lessOrEqualThan = val.compareTo(endRow) == -1;

			if (greaterOrEqualThan && lessOrEqualThan) {
				rangeValues.add(val);
			}
		}

		return rangeValues;

	}

	public void validateResults(List<BigInteger> valuesInRange,
			List<Result> results) {

		Set<BigInteger> valueSet = new HashSet<BigInteger>(valuesInRange);

		assertEquals(valuesInRange.size(), results.size());

		for (Result res : results) {
			byte[] resValue = res.getRow();
			assertEquals(true, valueSet.contains(new BigInteger(resValue)));
		}
	}

	@Override
	public void searchEndpointComparision(Dealer dealer,
			List<BigInteger> values, TestClusterTables tables, int nbits)
			throws InvalidNumberOfBits, InvalidSecretValue, Throwable {

		sortValues(values);
		for (BigInteger val : values) {
			System.out.println("The value is " + val);
		}

		setStartKey(values);
		setStopKey(values);

		byte[] startRow = null;
		byte[] stopRow = null;

		if (startKey != null) {
			startRow = startKey.toByteArray();
		}

		if (stopKey != null) {
			stopRow = stopKey.toByteArray();
		} else {
			stopRow = null;
		}
		//System.out.println("going to call scan endpoint with null keys");
		//List<Result> results = tables.scanEndpoint(nbits, null, null, 1,
                //			config, dealer);

		//System.out.println("Results sie is " + results.size());
		
		 List<Result> results = tables.scanEndpoint(nbits, startRow, stopRow, 1, config, dealer);
		 
		List<BigInteger> subsetValues = getKeysInRange(values, startKey,
				stopKey);
		System.out.println(subsetValues);
		validateResults(subsetValues, results);

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
