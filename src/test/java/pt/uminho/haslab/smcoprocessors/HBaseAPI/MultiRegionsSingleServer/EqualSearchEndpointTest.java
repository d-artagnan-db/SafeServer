package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServer;

public class EqualSearchEndpointTest
		extends
			pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegion.EqualSearchEndpointTest {
	private final static int NREGIONSERVERS = 1;
	private final static int NREGIONS = 2;

	public EqualSearchEndpointTest() throws Exception {
		super();
	}

	@Override
	protected int getNumberOfRegionServers() {
		return NREGIONSERVERS;
	}

	@Override
	protected int getNumberOfRegions() {
		return NREGIONS;
	}
}
