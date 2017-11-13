package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionsSingleServerFilters;

public class ScanFilterTest extends pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.ScanFilterTest {

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
