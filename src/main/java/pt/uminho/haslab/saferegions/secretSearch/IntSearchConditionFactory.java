package pt.uminho.haslab.saferegions.secretSearch;


import pt.uminho.haslab.saferegions.SmpcConfiguration;

import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class IntSearchConditionFactory extends SearchConditionFactory{

    public IntSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values, SmpcConfiguration config) {
        super(op, nBits, values, config);
    }

    @Override
    SearchCondition equalSearchValue() {
        return new IntSearchValue(nBits, value, Equal, config);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new IntSearchValue(nBits, value, GreaterOrEqualThan, config);
    }
}
