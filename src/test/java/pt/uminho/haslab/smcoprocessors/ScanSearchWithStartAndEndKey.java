package pt.uminho.haslab.smcoprocessors;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

public class ScanSearchWithStartAndEndKey extends ScanSearchEndpointTest {

	private final Random rand;

	public ScanSearchWithStartAndEndKey() throws Exception {
		super();
		rand = new Random();

	}

	@Override
	public void setStartKey(List<BigInteger> values) {
		startKey = values.get(rand.nextInt(values.size()));
	}

	@Override
	public void setStopKey(List<BigInteger> values) {
		stopKey = values.get(rand.nextInt(values.size()));

		BigInteger firstKey = startKey;
		BigInteger secondKey = stopKey;

		if (firstKey.compareTo(secondKey) == 1) {
			startKey = secondKey;
			stopKey = firstKey;
		}

	}

}
