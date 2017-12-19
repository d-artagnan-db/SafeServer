package pt.uminho.haslab.saferegions.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.*;

public class ComposedSearchValue extends AbstractSearchValue {

	static final Log LOG = LogFactory.getLog(ComposedSearchValue.class
			.getName());

	private final SearchCondition val1;
	private final SearchCondition val2;

	private final Condition parentCondition;

	public ComposedSearchValue(Condition condition, SearchCondition val1,
			SearchCondition val2, Condition parentCondition) {

		// TODO: check if conditions are on the same column;
		super(condition);
		this.val1 = val1;
		this.val2 = val2;
		this.parentCondition = parentCondition;
	}

	public void evaluateCondition(List<byte[]> value, List<byte[]> rowID,
			SharemindPlayer p) {
		val1.evaluateCondition(value, rowID, p);
		val2.evaluateCondition(value, rowID, p);
	}

	public Condition getCondition() {
		return this.condition;
	}

	public boolean getRowClassification(byte[] row) {
		Boolean res1 = val1.getRowClassification(row);
		Boolean res2 = val2.getRowClassification(row);
		return cmpVals(res1, res2);
	}

	private boolean cmpVals(Boolean v1, Boolean v2) {
		if (condition == And) {
			return v1 && v2;
		} else if (condition == Or) {
			return v1 || v2;
		} else if (condition == Xor) {
			return v1 ^ v2;
		}
		return false;
	}

	public void clearSearchIndexes() {
		val1.clearSearchIndexes();
		val2.clearSearchIndexes();
	}

	public List<Boolean> getClassificationList() {
		List<Boolean> results1 = val1.getClassificationList();
		List<Boolean> results2 = val2.getClassificationList();
		List<Boolean> result = new ArrayList<Boolean>();

		for (int i = 0; i < results1.size(); i++) {
			Boolean b1 = results1.get(i);
			Boolean b2 = results2.get(i);
			results1.add(cmpVals(b1, b2));
		}

		return result;
	}

	public SearchCondition getVal1() {
		return val1;
	}

	public SearchCondition getVal2() {
		return val2;
	}

	public Condition getParentCondition() {
		return this.parentCondition;
	}

}
