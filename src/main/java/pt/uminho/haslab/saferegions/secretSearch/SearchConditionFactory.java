package pt.uminho.haslab.saferegions.secretSearch;

import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.*;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Not;

public abstract class SearchConditionFactory {

    protected final SearchCondition.Condition op;
    protected final int nBits;
    protected final List<byte[]> value;

    public SearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values){

        this.op = op;
        this.nBits = nBits;
        this.value = values;
    }

    abstract SearchCondition  equalSearchValue();

    abstract SearchCondition  gteSearchValue();

    public SearchCondition conditionTransformer() {
        switch (op) {
            case Equal :
                return equalSearchValue();
            case GreaterOrEqualThan :
                return gteSearchValue();
            case Greater :
                SearchCondition equal = equalSearchValue();
                SearchCondition notEqual = new UnarySearchValue(Not, equal,
                        Equal);
                SearchCondition greaterEqualThan = gteSearchValue();
                return new ComposedSearchValue(And, notEqual, greaterEqualThan,
                        Greater);
            case Less :
                greaterEqualThan = gteSearchValue();
                return new UnarySearchValue(Not, greaterEqualThan,
                        GreaterOrEqualThan);
            case LessOrEqualThan :
                greaterEqualThan = gteSearchValue();
                equal = equalSearchValue();
                SearchCondition notGreater = new UnarySearchValue(Not,
                        greaterEqualThan, GreaterOrEqualThan);
                return new ComposedSearchValue(Xor, equal, notGreater,
                        LessOrEqualThan);
            case NotEqual :
                equal = equalSearchValue();
                return new UnarySearchValue(Not, equal, Equal);

        }
        return null;

    }
}
