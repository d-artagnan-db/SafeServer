package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LessOrEqualThanColumnValueFilterTest extends
        AbsColumnValueFilter {

    public LessOrEqualThanColumnValueFilterTest() throws Exception {
        super();
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.LESS_OR_EQUAL;
    }
}
