package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.List;
import java.util.Random;


public abstract class AbsColumnValueFilterTest
		extends
		AbstractUnprotectedFilterTest {
	private Random randomGenerator;


	public AbsColumnValueFilterTest() throws Exception {
		super();
		randomGenerator = new Random();

	}

	protected abstract CompareFilter.CompareOp getComparator();

	protected Filter getFilterOnUnprotectedColumn() {
		String cf = "User";
		String cq = "Name";
		List<byte[]> values = this.generatedValues.get(cf).get(cq);

		int indexChosen = randomGenerator.nextInt(values.size());
		LOG.debug("Index chosen was "+ indexChosen);
		byte[] val = values.get(indexChosen);
		LOG.debug("Random value chosen  was "+  new String(val));

		return new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(),
				getComparator(), val);
	}

	protected Filter getFilterOnProtectedColumn(){
		return getFilterOnUnprotectedColumn();
	}



}
