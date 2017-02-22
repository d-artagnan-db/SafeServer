package pt.uminho.haslab.smcoprocessors;

import pt.uminho.haslab.testingutils.ScanValidator;

public class ScanSearchWithStopKey extends ScanSearchEndpointTest {

	public ScanSearchWithStopKey() throws Exception {
		super();
	}

	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return null;
	}

	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return validator.generateStopKey();
	}

}
