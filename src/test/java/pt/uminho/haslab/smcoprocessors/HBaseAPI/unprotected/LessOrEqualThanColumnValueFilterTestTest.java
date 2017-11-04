package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LessOrEqualThanColumnValueFilterTestTest extends
        AbsColumnValueFilterTest {

    public LessOrEqualThanColumnValueFilterTestTest() throws Exception {
        super();
    }

    protected CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.LESS_OR_EQUAL;
    }
}
