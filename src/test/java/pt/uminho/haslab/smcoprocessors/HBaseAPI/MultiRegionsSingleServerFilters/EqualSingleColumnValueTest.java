package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerFilters;

import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.EqualSingleColumnValueFilterTest;

public class EqualSingleColumnValueTest extends EqualSingleColumnValueFilterTest {

    public EqualSingleColumnValueTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }

}
