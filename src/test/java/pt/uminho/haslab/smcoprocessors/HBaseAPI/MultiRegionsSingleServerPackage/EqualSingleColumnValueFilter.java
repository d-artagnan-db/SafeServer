package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerPackage;

import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.EqualSingleColumnValueFilterTest;

public class EqualSingleColumnValueFilter extends EqualSingleColumnValueFilterTest {

    public EqualSingleColumnValueFilter() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }

}
