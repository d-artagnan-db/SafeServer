package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.LESS_OR_EQUAL;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

public class LessOrEqualThanSecretSearchTest extends SecretSearchTest {

	public LessOrEqualThanSecretSearchTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int valueNBits, byte[] value,
			int targetPlayer) {
		return AbstractSearchValue.conditionTransformer(LESS_OR_EQUAL,
				valueNBits, value, targetPlayer);

	}

	@Override
	protected int compareOriginalValues(BigInteger first, BigInteger second) {
		int comparisonResult = first.compareTo(second);
		return comparisonResult == -1 || comparisonResult == 0 ? 1 : 0;
	}

}
