package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionMultiServerFilters;

public class GreaterThanTest extends pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.GreaterThanTest {

    public GreaterThanTest() throws Exception {
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
