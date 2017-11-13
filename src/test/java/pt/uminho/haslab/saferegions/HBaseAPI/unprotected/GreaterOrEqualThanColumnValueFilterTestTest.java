package pt.uminho.haslab.saferegions.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterOrEqualThanColumnValueFilterTestTest extends
        AbsColumnValueFilterTest {

    public GreaterOrEqualThanColumnValueFilterTestTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 10;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}
