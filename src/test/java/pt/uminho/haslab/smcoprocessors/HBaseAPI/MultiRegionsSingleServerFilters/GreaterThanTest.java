package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerFilters;

public class GreaterThanTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.GreaterThanTest {

    public GreaterThanTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }

}
