package pt.uminho.haslab.saferegions.secretSearch;


import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class IntSearchConditionFactory extends SearchConditionFactory{

    public IntSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values) {
        super(op, nBits, values);
    }

    @Override
    SearchCondition equalSearchValue() {
        return new IntSearchValue(nBits, value, Equal);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new IntSearchValue(nBits, value, GreaterOrEqualThan);
    }
}
