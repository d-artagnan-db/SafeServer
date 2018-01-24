package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.secretSearch.IntSearchConditionFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.io.IOException;
import java.util.List;

public class IntRegionServerSim extends RegionServerSim {

    public IntRegionServerSim(int playerID, SearchCondition.Condition condition, int nBits, List<List<byte[]>> firstInputs, List<List<byte[]>> secondInputs) throws IOException {
        super(playerID, condition, nBits, firstInputs, secondInputs);
    }

    @Override
    protected SearchCondition getSearchCondition(List<byte[]> secTwo) {
        return new IntSearchConditionFactory(cond, nBits + 1, secTwo, searchConf, "test", "test").conditionTransformer();
    }
}
