package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Greater;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

public class GreaterThanSecretSearchTest extends SecretSearchTest {

	public GreaterThanSecretSearchTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int valueNBits, byte[] value,
			int targetPlayer) {
		return AbstractSearchValue.conditionTransformer(Greater, valueNBits,
				value, targetPlayer);

	}

	@Override
	protected int compareOriginalValues(BigInteger first, BigInteger second) {
		int comparisonResult = first.compareTo(second);
		return comparisonResult == 1 ? 1 : 0;
	}

}
