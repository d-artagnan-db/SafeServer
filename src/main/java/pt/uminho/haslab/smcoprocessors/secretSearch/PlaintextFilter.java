package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.hadoop.hbase.filter.BinaryComparator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlaintextFilter extends AbstractSearchValue {

	private final Map<byte[], byte[]> rows;

	private final BinaryComparator comparator;

	private final byte[] cmpValue;

	public PlaintextFilter(byte[] value, Condition condition) {
		super(condition);
		cmpValue = value;
		rows = new HashMap<byte[], byte[]>();
		comparator = new BinaryComparator(value);

	}

	public void evaluateCondition(List<byte[]> value, List<byte[]> rowID,
			SharemindPlayer p) {
		for (int i = 0; i < rows.size(); i++) {
			rows.put(value.get(i), rowID.get(i));
		}
	}

	public boolean getRowClassification(byte[] row) {
		byte[] val = rows.get(row);
		switch (condition) {
			case Equal :
				return comparator.compareTo(val, 0, row.length) == 0;
			case GreaterOrEqualThan :
				return comparator.compareTo(val, 0, row.length) == 1;
			case LessOrEqualThan :
				return comparator.compareTo(val, 0, row.length) == -1;
			case Greater :
				int gte = comparator.compareTo(val, 0, row.length);
				boolean g_eq = Arrays.equals(cmpValue, val);
				return gte == 1 && !g_eq;
			case Less :
				int loe = comparator.compareTo(val, 0, row.length);
				boolean l_eq = Arrays.equals(cmpValue, val);
				return loe == -1 && !l_eq;
			default :
				throw new IllegalStateException("Operation Not Supported");
		}

	}

	public void clearSearchIndexes() {

	}

	public List<Boolean> getClassificationList() {
		throw new UnsupportedOperationException(
				"Operation is only relevant for  batch operators.");
	}
}
