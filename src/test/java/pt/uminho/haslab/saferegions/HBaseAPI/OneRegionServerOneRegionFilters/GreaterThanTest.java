package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterThanTest extends AbsSingleColumnValueFilterTest {
    public GreaterThanTest() throws Exception {
        super();
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER;
    }
}
