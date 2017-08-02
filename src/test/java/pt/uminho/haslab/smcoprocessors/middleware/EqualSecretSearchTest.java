package pt.uminho.haslab.smcoprocessors.middleware;

import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;

public class EqualSecretSearchTest extends SecretSearchTest {

    public EqualSecretSearchTest(List<Integer> nbits,
                                 List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected SearchCondition getSearchCondition(int valueNBits, byte[] value,
                                                 int targetPlayer) {
        List<byte[]> val = new ArrayList<byte[]>();
        val.add(value);
        return AbstractSearchValue.conditionTransformer(Equal, valueNBits, val,
                targetPlayer);
    }

    @Override
    protected int compareOriginalValues(BigInteger first, BigInteger second) {
        boolean comparisonResult = first.equals(second);
        return comparisonResult ? 1 : 0;
    }

}
