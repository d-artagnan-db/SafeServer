package pt.uminho.haslab.saferegions.secretSearch.concurrent;

import pt.uminho.haslab.saferegions.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Less;

public class ConcurrentBatchLesserThanSecretSearchTest
		extends
			ConcurrentSecretSearchTest {

	public ConcurrentBatchLesserThanSecretSearchTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	protected SearchCondition getSearchCondition(int nBits,
			List<byte[]> firstValueSecret) {
		return AbstractSearchValue.conditionTransformer(Less, nBits,
				firstValueSecret);
	}

	protected List<Boolean> getSearchExpectedResult(Integer request) {
		List<BigInteger> secretOne = valuesOne.get(request);
		List<BigInteger> secretTwo = valuesTwo.get(request);
		List<Boolean> bool = new ArrayList<Boolean>();
		for (int i = 0; i < secretOne.size(); i++) {
			int comparisonResult = secretOne.get(i).compareTo(secretTwo.get(i));
			boolean expectedResult = comparisonResult == -1;
			bool.add(expectedResult);
		}
		return bool;
	}
}
