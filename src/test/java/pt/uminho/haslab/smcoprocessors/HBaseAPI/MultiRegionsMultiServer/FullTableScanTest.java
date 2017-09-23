package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsMultiServer;

public class FullTableScanTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegion.FullTableScanTest {

    private final static int NREGIONSERVERS = 2;
    private final static int NREGIONS = 2;

    public FullTableScanTest() throws Exception {
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
