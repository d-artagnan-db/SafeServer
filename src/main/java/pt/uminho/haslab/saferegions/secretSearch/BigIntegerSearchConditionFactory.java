package pt.uminho.haslab.saferegions.secretSearch;

import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class BigIntegerSearchConditionFactory extends SearchConditionFactory{

    public BigIntegerSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values) {
        super(op, nBits, values);
    }

    @Override
    SearchCondition equalSearchValue() {
        return new BigIntegerSearchValue(nBits, value, Equal);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new BigIntegerSearchValue(nBits, value, GreaterOrEqualThan);
    }
}
