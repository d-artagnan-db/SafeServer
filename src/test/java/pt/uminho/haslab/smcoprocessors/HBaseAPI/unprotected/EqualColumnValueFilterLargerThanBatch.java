package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualColumnValueFilterLargerThanBatch extends
        AbsColumnValueFilter {

    public EqualColumnValueFilterLargerThanBatch() throws Exception {
        super();
    }

    protected long getNumberOfRecords() {
        return 35;
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }
}
