package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Not;

public class UnarySearchValue extends AbstractSearchValue {
	static final Log LOG = LogFactory.getLog(UnarySearchValue.class.getName());

	private final SearchCondition searchCondition;

	private final Condition parentCondition;

	public UnarySearchValue(Condition condition,
			SearchCondition searchCondition, Condition parentCondition) {
		// TODO: check if condition is on the same column;
		super(condition);
		this.searchCondition = searchCondition;
		this.parentCondition = parentCondition;
	}

	public void evaluateCondition(List<byte[]> value, List<byte[]> rowID,
			SharemindPlayer p) {
		searchCondition.evaluateCondition(value, rowID, p);
	}

	public Condition getCondition() {
		return this.condition;
	}

	public boolean getRowClassification(byte[] row) {
		boolean res = searchCondition.getRowClassification(row);
		return (condition == Not) != res;

	}

	public void clearSearchIndexes() {
		searchCondition.clearSearchIndexes();
	}

	public List<Boolean> getClassificationList() {
		List<Boolean> res = searchCondition.getClassificationList();

		if (condition == Not) {
			List<Boolean> negatedRes = new ArrayList<Boolean>();

			for (Boolean r : res) {
				negatedRes.add(!r);
			}
			return negatedRes;
		} else {
			return res;
		}
	}

	public SearchCondition getSearchCondition() {
		return searchCondition;
	}

	public Condition getParentCondition() {
		return this.parentCondition;
	}
}
