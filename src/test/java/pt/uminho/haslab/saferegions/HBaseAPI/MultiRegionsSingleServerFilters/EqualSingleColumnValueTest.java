package pt.uminho.haslab.saferegions.HBaseAPI.MultiRegionsSingleServerFilters;

import pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters.EqualSingleColumnValueFilterTest;

public class EqualSingleColumnValueTest extends EqualSingleColumnValueFilterTest {

    public EqualSingleColumnValueTest() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 4;
    }

    protected long getNumberOfRecords() {
        return 1000;
    }
    
    public static void main(String[] args){
    }
}
