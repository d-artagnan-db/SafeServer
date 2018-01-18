package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;

import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class LongSearchConditionFactory extends SearchConditionFactory {

    public LongSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values, SmpcConfiguration configuration) {
        super(op, nBits, values, configuration);
    }

    @Override
    SearchCondition equalSearchValue() {
        return new LongSearchValue(nBits, value, Equal, config);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new LongSearchValue(nBits, value, GreaterOrEqualThan, config);
    }
}
