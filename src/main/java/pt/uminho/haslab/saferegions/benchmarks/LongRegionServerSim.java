package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.secretSearch.LongSearchConditionFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.io.IOException;
import java.util.List;

public class LongRegionServerSim extends RegionServerSim {
    public LongRegionServerSim(int playerID, SearchCondition.Condition condition, int nBits, List<List<byte[]>> firstInputs, List<List<byte[]>> secondInputs) throws IOException {
        super(playerID, condition, nBits, firstInputs, secondInputs);
    }

    @Override
    protected SearchCondition getSearchCondition(List<byte[]> secTwo) {
        return new LongSearchConditionFactory(cond, nBits + 1, secTwo, searchConf, "test", "test").conditionTransformer();
    }
}
