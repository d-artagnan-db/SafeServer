package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionsSingleServerFilters;

import pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.IntEqualSingleColumnValueFilerTest;

public class IntEqualSingleColumnValueTest extends IntEqualSingleColumnValueFilerTest {
    public IntEqualSingleColumnValueTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 4;
    }

    protected long getNumberOfRecords() {
        return 1000;
    }
}
