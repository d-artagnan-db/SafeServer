package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LessThanSingleColumnValueFilterTest extends AbsSingleColumnValueFilterTest {

    public LessThanSingleColumnValueFilterTest() throws Exception {
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.LESS;
    }
}
