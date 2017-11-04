package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualColumnValueFilterTestSmallerThanBatchSizeTest extends
        AbsColumnValueFilterTest {

    public EqualColumnValueFilterTestSmallerThanBatchSizeTest() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 5;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }
}
