package pt.uminho.haslab.smcoprocessors;

import pt.uminho.haslab.testingutils.ScanValidator;

public class ScanSearchWithStartAndEndKeyTest extends ScanSearchEndpointTest {

	public ScanSearchWithStartAndEndKeyTest() throws Exception {
		super();
	}

	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return validator.generateStartKey();
	}

	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return validator.generateStopKey();
	}

}
