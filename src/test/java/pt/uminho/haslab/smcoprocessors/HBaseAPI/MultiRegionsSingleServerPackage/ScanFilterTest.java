package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionsSingleServerPackage;

public class ScanFilterTest extends pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.ScanFilterTest {

    public ScanFilterTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 10;
    }
}
