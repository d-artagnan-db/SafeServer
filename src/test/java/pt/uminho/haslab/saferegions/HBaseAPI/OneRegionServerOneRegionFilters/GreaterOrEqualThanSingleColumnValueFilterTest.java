package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterOrEqualThanSingleColumnValueFilterTest extends AbsSingleColumnValueFilterTest {

    public GreaterOrEqualThanSingleColumnValueFilterTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 10;
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}
