package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;

import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class LongSearchConditionFactory extends SearchConditionFactory {

    private final String column;
    private final String regionIdent;

    public LongSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values, SmpcConfiguration configuration, String column, String regionIdent) {
        super(op, nBits, values, configuration);
        this.column = column;
        this.regionIdent = regionIdent;
    }

    @Override
    SearchCondition equalSearchValue() {
        return new LongSearchValue(nBits, value, Equal, config, column, regionIdent);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new LongSearchValue(nBits, value, GreaterOrEqualThan, config, column, regionIdent);
    }
}
