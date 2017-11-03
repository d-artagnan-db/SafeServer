package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterOrEqualThanColumnValueFilterTest extends
        AbsColumnValueFilter {

    public GreaterOrEqualThanColumnValueFilterTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 10;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}
