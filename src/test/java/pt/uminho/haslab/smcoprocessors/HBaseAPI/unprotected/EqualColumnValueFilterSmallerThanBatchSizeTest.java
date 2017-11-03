package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualColumnValueFilterSmallerThanBatchSizeTest extends
        AbsColumnValueFilter {

    public EqualColumnValueFilterSmallerThanBatchSizeTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 5;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }
}
