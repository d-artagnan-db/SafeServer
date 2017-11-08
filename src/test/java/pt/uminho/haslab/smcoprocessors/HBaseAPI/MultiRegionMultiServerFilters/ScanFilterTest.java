package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionMultiServerFilters;

public class ScanFilterTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.ScanFilterTest {

    public ScanFilterTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 4;
    }

    protected long getNumberOfRecords() {
        return 800;
    }

    protected int getNumberOfRegionsServers() {
        return 2;
    }
}
