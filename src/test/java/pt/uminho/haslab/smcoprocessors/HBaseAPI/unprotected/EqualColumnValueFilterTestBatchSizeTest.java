package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class EqualColumnValueFilterTestBatchSizeTest
		extends
        AbsColumnValueFilterTest {

	public EqualColumnValueFilterTestBatchSizeTest() throws Exception {
		super();
	}

	protected long getNumberOfRecords() {
		return 20;
	}

	protected CompareFilter.CompareOp getComparator() {
		return CompareFilter.CompareOp.EQUAL;
	}
}
