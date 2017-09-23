package pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegion;

import pt.uminho.haslab.testingutils.ScanValidator;

public class FullTableScanTest extends ScanSearchEndpointTest {

	public FullTableScanTest() throws Exception {
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
