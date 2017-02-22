package pt.uminho.haslab.smcoprocessors;

import pt.uminho.haslab.testingutils.ScanValidator;

public class FullTableScan extends ScanSearchEndpointTest {

	public FullTableScan() throws Exception {
		super();
	}
	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return null;
	}

	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return null;
	}

}
