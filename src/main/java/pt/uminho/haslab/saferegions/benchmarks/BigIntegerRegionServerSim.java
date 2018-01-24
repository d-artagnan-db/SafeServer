package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.secretSearch.BigIntegerSearchConditionFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.io.IOException;
import java.util.List;

public class BigIntegerRegionServerSim  extends RegionServerSim{

    public BigIntegerRegionServerSim(int playerID, SearchCondition.Condition condition, int nBits, List<List<byte[]>> firstInputs, List<List<byte[]>> secondInputs) throws IOException {
        super(playerID, condition, nBits, firstInputs, secondInputs);
    }

    @Override
    protected SearchCondition getSearchCondition(List<byte[]> secTwo) {
        return new BigIntegerSearchConditionFactory(cond, nBits + 1, secTwo, searchConf).conditionTransformer();
    }
}
