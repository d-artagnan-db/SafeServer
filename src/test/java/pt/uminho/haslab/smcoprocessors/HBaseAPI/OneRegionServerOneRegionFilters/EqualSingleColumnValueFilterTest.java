package pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualSingleColumnValueFilterTest extends AbsSingleColumnValueFilterTest {

    public EqualSingleColumnValueFilterTest() throws Exception {
        super();
    }

    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }
}
