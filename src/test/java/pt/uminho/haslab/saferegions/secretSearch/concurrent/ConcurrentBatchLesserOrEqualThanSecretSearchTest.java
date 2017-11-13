package pt.uminho.haslab.saferegions.secretSearch.concurrent;

import pt.uminho.haslab.saferegions.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.LessOrEqualThan;

public class ConcurrentBatchLesserOrEqualThanSecretSearchTest
		extends
			ConcurrentSecretSearchTest {

	public ConcurrentBatchLesserOrEqualThanSecretSearchTest(
			List<Integer> nbits, List<List<BigInteger>> valuesOne,
			List<List<BigInteger>> valuesTwo) throws IOException,
			InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int nBits,
			List<byte[]> firstValueSecret) {
		return AbstractSearchValue.conditionTransformer(LessOrEqualThan, nBits,
				firstValueSecret);
	}

	@Override
	protected List<Boolean> getSearchExpectedResult(Integer request) {
		List<BigInteger> secretOne = valuesOne.get(request);
		List<BigInteger> secretTwo = valuesTwo.get(request);
		List<Boolean> bool = new ArrayList<Boolean>();

		for (int i = 0; i < secretOne.size(); i++) {
			int comparisonResult = secretOne.get(i).compareTo(secretTwo.get(i));
			boolean expectedResult = comparisonResult == 0
					|| comparisonResult == -1;
			bool.add(expectedResult);
		}
		return bool;

	}

}
