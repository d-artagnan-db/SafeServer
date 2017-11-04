package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerPackage;

public class LessOrEqualThanTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.LessOrEqualThanTest {

    public LessOrEqualThanTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
