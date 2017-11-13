package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionsSingleServerFilters;

public class WhileMatchFilterTest extends pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.WhileMatchFilterTest {

    public WhileMatchFilterTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 800;
    }
}
