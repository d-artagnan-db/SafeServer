package pt.uminho.haslab.saferegions.secretSearch;


import pt.uminho.haslab.saferegions.SmpcConfiguration;

import java.math.BigInteger;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class IntSearchConditionFactory extends SearchConditionFactory{

    private final String column;
    private final String regionIdentifier;
    public IntSearchConditionFactory(SearchCondition.Condition op, int nBits, List<byte[]> values, SmpcConfiguration config, String column, String regionIdentifier) {
        super(op, nBits, values, config);
        this.column = column;
        this.regionIdentifier = regionIdentifier;
    }

    @Override
    SearchCondition equalSearchValue() {
        return new IntSearchValue(nBits, value, Equal, config, column, regionIdentifier);
    }

    @Override
    SearchCondition gteSearchValue() {
        return new IntSearchValue(nBits, value, GreaterOrEqualThan, config, column, regionIdentifier);
    }
}
