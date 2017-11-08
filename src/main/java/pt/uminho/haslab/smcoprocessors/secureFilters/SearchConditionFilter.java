package pt.uminho.haslab.smcoprocessors.secureFilters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.Column;

import java.util.Arrays;
import java.util.List;

public class SearchConditionFilter implements SecureFilter {
	protected SearchCondition condition;
	protected Column col;

	public SearchConditionFilter(SearchCondition condition, Column col) {
		this.condition = condition;
		this.col = col;

	}

	public boolean filterRow(List<Cell> cells) {
		for (Cell cell : cells) {
			byte[] row = CellUtil.cloneRow(cell);
            byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);


			if (Arrays.equals(col.getCf(), cf)
					&& Arrays.equals(col.getCq(), cq)) {
				if (this.condition.getRowClassification(row)) {
					return true;
				}
			}
		}
		return false;
	}

	public void reset() {
		condition.clearSearchIndexes();
	}

	public SearchCondition getCondition() {
		return condition;
	}
}
