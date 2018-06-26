package pt.uminho.haslab.saferegions.secureFilters;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.saferegions.secureRegionScanner.Column;

import java.util.Arrays;
import java.util.List;

public class SearchConditionFilter implements SecureFilter {
    private static final Log LOG = LogFactory.getLog(SearchConditionFilter.class
            .getName());
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
