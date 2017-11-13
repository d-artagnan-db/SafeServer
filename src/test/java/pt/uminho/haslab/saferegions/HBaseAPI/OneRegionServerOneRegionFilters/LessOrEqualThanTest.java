package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LessOrEqualThanTest extends AbsSingleColumnValueFilterTest {

    public LessOrEqualThanTest() throws Exception {
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.LESS_OR_EQUAL;
    }
}
