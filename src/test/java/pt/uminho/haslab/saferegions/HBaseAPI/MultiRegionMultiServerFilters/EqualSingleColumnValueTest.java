package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionMultiServerFilters;

import pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.EqualSingleColumnValueFilterTest;

public class EqualSingleColumnValueTest extends EqualSingleColumnValueFilterTest {

    public EqualSingleColumnValueTest() throws Exception {
        super();
    }
    protected int getNumberOfRegions() {
        return 4;
    }

    protected long getNumberOfRecords() {
        return 1000;
    }

    protected int getNumberOfRegionsServers(){
        return 2;
    }
}
