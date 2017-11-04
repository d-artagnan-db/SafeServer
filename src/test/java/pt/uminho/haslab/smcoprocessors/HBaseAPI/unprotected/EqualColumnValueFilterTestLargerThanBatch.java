package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualColumnValueFilterTestLargerThanBatch extends
        AbsColumnValueFilterTest {

    public EqualColumnValueFilterTestLargerThanBatch() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 35;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }
}
