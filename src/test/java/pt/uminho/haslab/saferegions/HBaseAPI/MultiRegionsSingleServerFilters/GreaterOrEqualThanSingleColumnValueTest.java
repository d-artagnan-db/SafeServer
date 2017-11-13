package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionsSingleServerFilters;


import pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.GreaterOrEqualThanSingleColumnValueFilterTest;

public class GreaterOrEqualThanSingleColumnValueTest extends GreaterOrEqualThanSingleColumnValueFilterTest {

    public GreaterOrEqualThanSingleColumnValueTest() throws Exception {
        super();
    }
    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
