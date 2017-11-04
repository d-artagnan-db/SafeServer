package pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterOrEqualThanSingleColumnValueFilterTest extends AbsSingleColumnValueFilterTest {

    public GreaterOrEqualThanSingleColumnValueFilterTest() throws Exception {
        super();
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}
