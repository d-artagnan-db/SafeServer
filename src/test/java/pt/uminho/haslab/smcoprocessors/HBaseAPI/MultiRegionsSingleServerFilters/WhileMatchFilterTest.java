package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerFilters;

public class WhileMatchFilterTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.WhileMatchFilterTest{

    public WhileMatchFilterTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
