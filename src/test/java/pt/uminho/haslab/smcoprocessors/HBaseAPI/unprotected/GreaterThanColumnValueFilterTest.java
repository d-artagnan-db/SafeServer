package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterThanColumnValueFilterTest extends
        AbsColumnValueFilter {

    public GreaterThanColumnValueFilterTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 10;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER;
    }
}
