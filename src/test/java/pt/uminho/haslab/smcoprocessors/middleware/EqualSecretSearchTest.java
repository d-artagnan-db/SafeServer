package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

public class EqualSecretSearchTest extends SecretSearchTest {

	public EqualSecretSearchTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int valueNBits, byte[] value,
			int targetPlayer) {
		return AbstractSearchValue.conditionTransformer(EQUAL, valueNBits,
				value, targetPlayer);
	}

	@Override
	protected int compareOriginalValues(BigInteger first, BigInteger second) {
		boolean comparisonResult = first.equals(second);
		return comparisonResult ? 1 : 0;
	}

}
