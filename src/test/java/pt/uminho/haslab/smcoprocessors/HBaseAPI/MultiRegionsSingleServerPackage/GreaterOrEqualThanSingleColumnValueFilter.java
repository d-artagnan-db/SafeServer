package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerPackage;


import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.GreaterOrEqualThanSingleColumnValueFilterTest;

public class GreaterOrEqualThanSingleColumnValueFilter extends GreaterOrEqualThanSingleColumnValueFilterTest {

    public GreaterOrEqualThanSingleColumnValueFilter() throws Exception {
        super();
    }
    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
