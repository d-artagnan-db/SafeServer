package pt.uminho.haslab.saferegions.secureFilters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.saferegions.secureRegionScanner.Column;

import java.util.Arrays;
import java.util.List;

public class SecureSingleColumnValueFilter extends SecureSimpleValueFilter {

	private Column col;

	public SecureSingleColumnValueFilter(Column col,
			SearchCondition.Condition cond, ByteArrayComparable comparable) {
		super(cond, comparable);
		this.col = col;
	}

	public boolean filterRow(List<Cell> cells) {

		for (Cell cell : cells) {
			byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);

			if (Arrays.equals(col.getCf(), cf)
					&& Arrays.equals(col.getCq(), cq)) {
				byte[] val = CellUtil.cloneValue(cell);
				return compareValue(val);
			}
		}
		return false;
	}

}
