package pt.uminho.haslab.smcoprocessors.HBaseAPI.MultiRegionMultiServerPackage;

import pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters.EqualSingleColumnValueFilterTest;

public class EqualSingleColumnValueFilter extends EqualSingleColumnValueFilterTest {

    public EqualSingleColumnValueFilter() throws Exception {
        super();
    }

    protected int getNumberOfRegions() {
        return 2;
    }

    protected long getNumberOfRecords() {
        return 400;
    }

    protected int getNumberOfRegionsServers(){
        return 2;
    }
}
