package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionMultiServerFilters;

import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.LessThanSingleColumnValueFilterTest;

public class LessThanTest extends LessThanSingleColumnValueFilterTest {

    public LessThanTest() throws Exception {
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
