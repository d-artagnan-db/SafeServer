package pt.uminho.haslab.smcoprocessors.secretSearch.concurrent;

import pt.uminho.haslab.smcoprocessors.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Equal;

public class ConcurrentBatchEqualSecretSearchTest
		extends
			ConcurrentSecretSearchTest {

	public ConcurrentBatchEqualSecretSearchTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	@Override
	protected SearchCondition getSearchCondition(int nBits,
			List<byte[]> firstValueSecret) {
		return AbstractSearchValue.conditionTransformer(Equal, nBits,
				firstValueSecret);
	}

	@Override
	protected List<Boolean> getSearchExpectedResult(Integer request) {
		List<BigInteger> secretOne = valuesOne.get(request);
		List<BigInteger> secretTwo = valuesTwo.get(request);
		List<Boolean> bool = new ArrayList<Boolean>();
		for (int i = 0; i < secretOne.size(); i++) {
			bool.add(secretOne.get(i).equals(secretTwo.get(i)));
		}
		return bool;
	}

}
