package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerFilters;

import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.LessThanSingleColumnValueFilterTest;

public class LessThanTest  extends LessThanSingleColumnValueFilterTest{

    public LessThanTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
