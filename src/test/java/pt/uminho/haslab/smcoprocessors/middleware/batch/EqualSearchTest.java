package pt.uminho.haslab.smcoprocessors.middleware.batch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

public class EqualSearchTest extends SecretSearchTest {

	public EqualSearchTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected boolean compareOriginalValues(BigInteger first, BigInteger second) {
		return first.equals(second);
	}

	@Override
	protected List<byte[]> testingProtocol(List<byte[]> originalSecrets,
			List<byte[]> cmpSecrets) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected SearchCondition getSearchCondition(int valueNBits,
			List<byte[]> value, int targetPlayer) {
		return AbstractSearchValue.conditionTransformer(Equal, valueNBits,
				value, targetPlayer);
	}

}
