package pt.uminho.haslab.smcoprocessors;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

public class ScanSearchWithStartKey extends ScanSearchEndpointTest {

	private final Random rand;

	public ScanSearchWithStartKey() throws Exception {
		super();
		rand = new Random();

	}

	@Override
	public void setStartKey(List<BigInteger> values) {
		startKey = values.get(rand.nextInt(values.size()));
	}

	@Override
	public void setStopKey(List<BigInteger> values) {
		stopKey = null;
	}

}
