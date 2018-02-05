package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SearchValue extends AbstractSearchValue {

	protected final List<byte[]> value;

	protected final int nBits;

	protected final Map<String, Boolean> resultIndex;

	protected  final List<Boolean> resultsList;

    protected final SmpcConfiguration config;

    public SearchValue(int nBits, List<byte[]> value, Condition condition, SmpcConfiguration config) {
        super(condition);
		this.value = value;
		this.nBits = nBits;
		resultIndex = new HashMap<String, Boolean>();
		resultsList = new ArrayList<Boolean>();
        this.config = config;
    }

	public boolean getRowClassification(byte[] row) {
		if (resultIndex.isEmpty()) {
			throw new IllegalStateException(
					"The method evaluateCondition must be evaluated before using this method");
		}

		return resultIndex.get(new String(row));
	}

	public void clearSearchIndexes() {
		resultIndex.clear();
        resultsList.clear();
    }

	public List<Boolean> getClassificationList() {
		return resultsList;
	}


}
