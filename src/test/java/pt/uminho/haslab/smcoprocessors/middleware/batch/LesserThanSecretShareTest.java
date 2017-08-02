package pt.uminho.haslab.smcoprocessors.middleware.batch;

import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Less;

public class LesserThanSecretShareTest extends SecretSearchTest {

    public LesserThanSecretShareTest(List<Integer> nbits,
                                     List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected SearchCondition getSearchCondition(int valueNBits,
                                                 List<byte[]> value, int targetPlayer) {
        return AbstractSearchValue.conditionTransformer(Less, valueNBits,
                value, targetPlayer);
    }

    @Override
    protected boolean compareOriginalValues(BigInteger first, BigInteger second) {
        int comparisonResult = first.compareTo(second);
        return comparisonResult == -1;
    }

    @Override
    protected List<byte[]> testingProtocol(List<byte[]> originalSecrets,
                                           List<byte[]> cmpSecrets) {
        throw new UnsupportedOperationException("Not supported yet."); // To
        // change
        // body
        // of
        // generated
        // methods,
        // choose
        // Tools
        // |
        // Templates.
    }

}
