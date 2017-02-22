package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

public class ConcurrentEqualSearchTest extends ConcurrentSecretSearchTest {
	static final Log LOG = LogFactory.getLog(ConcurrentEqualSearchTest.class
			.getName());
	public ConcurrentEqualSearchTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int nBits,
			byte[] firstValueSecret, int i) {
		return AbstractSearchValue.conditionTransformer(Equal, nBits + 1,
				firstValueSecret, i);
	}

	@Override
	protected boolean getSearchExpectedResult(Integer request) {
		LOG.debug("Values size " + valuesOne.size() + " - request " + request);
		BigInteger secretOne = valuesOne.get(request);
		BigInteger secretTwo = valuesTwo.get(request);
		return secretOne.equals(secretTwo);
	}

}
