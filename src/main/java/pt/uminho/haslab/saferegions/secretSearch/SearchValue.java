package pt.uminho.haslab.saferegions.secretSearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SearchValue extends AbstractSearchValue {

	protected final List<byte[]> value;

	protected final int nBits;

	protected final Map<BigInteger, Boolean> resultIndex;

	protected  final List<Boolean> resultsList;

	public SearchValue(int nBits, List<byte[]> value, Condition condition) {
		super(condition);
		this.value = value;
		this.nBits = nBits;
		resultIndex = new HashMap<BigInteger, Boolean>();
		resultsList = new ArrayList<Boolean>();
	}

	public boolean getRowClassification(byte[] row) {
		if (resultIndex.isEmpty()) {
			throw new IllegalStateException(
					"The method evaluateCondition must be evaluated before using this method");
		}
		return resultIndex.get(new BigInteger(row));
	}

	public void clearSearchIndexes() {
		resultIndex.clear();
	}

	public List<Boolean> getClassificationList() {
		return resultsList;
	}


}
