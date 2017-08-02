package pt.uminho.haslab.smcoprocessors.middleware.batch;

import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class GreaterOrEqualThanTest extends SecretSearchTest {

    public GreaterOrEqualThanTest(List<Integer> nbits,
                                  List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected SearchCondition getSearchCondition(int valueNBits,
                                                 List<byte[]> value, int targetPlayer) {
        return AbstractSearchValue.conditionTransformer(GreaterOrEqualThan,
                valueNBits, value, targetPlayer);
    }

    @Override
    protected boolean compareOriginalValues(BigInteger first, BigInteger second) {
        int comparisonResult = first.compareTo(second);
        return comparisonResult == 1 || comparisonResult == 0;
    }

    @Override
    protected List<byte[]> testingProtocol(List<byte[]> originalSecrets,
                                           List<byte[]> cmpSecrets) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
