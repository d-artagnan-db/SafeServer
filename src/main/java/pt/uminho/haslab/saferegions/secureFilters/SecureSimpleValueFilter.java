package pt.uminho.haslab.saferegions.secureFilters;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.util.Arrays;

public abstract class SecureSimpleValueFilter implements SecureFilter {

    protected final SearchCondition.Condition cond;
    protected final ByteArrayComparable comparable;

    public SecureSimpleValueFilter(SearchCondition.Condition cond,
                                   ByteArrayComparable comparable) {
        this.cond = cond;
        this.comparable = comparable;
    }

    public void reset() {
    }

    protected boolean compareValue(byte[] val) {

        switch (cond) {
            case Equal:
                return comparable.compareTo(val) == 0;
            case Greater:
                return comparable.compareTo(val) < 0;
            case Less:
                return comparable.compareTo(val) > 0;
            case GreaterOrEqualThan:
                int gt = comparable.compareTo(val);
                boolean g_eq = Arrays.equals(comparable.getValue(), val);
                return gt < 0 || g_eq;
            case LessOrEqualThan:
                int lo = comparable.compareTo(val);
                boolean l_eq = Arrays.equals(comparable.getValue(), val);
                return lo > 0 || l_eq;
            default:
                throw new IllegalStateException("Operation Not Supported");
        }
    }

}
