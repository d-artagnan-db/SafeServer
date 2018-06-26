package pt.uminho.haslab.saferegions.secureFilters;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.util.List;

public class SecureIdentifierFilter extends SecureSimpleValueFilter {

    public SecureIdentifierFilter(SearchCondition.Condition cond,
                                  ByteArrayComparable comparator) {
        super(cond, comparator);
    }

    public boolean filterRow(List<Cell> row) {
        byte[] identifier = CellUtil.cloneRow(row.get(0));
        return compareValue(identifier);
    }
}
