package pt.uminho.haslab.smcoprocessors;

import pt.uminho.haslab.testingutils.ScanValidator;

public class ScanSearchWithStartKey extends ScanSearchEndpointTest {

	public ScanSearchWithStartKey() throws Exception {
		super();
	}

	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return validator.generateStartKey();
	}

	// return empty key;
	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return null;
	}

}
