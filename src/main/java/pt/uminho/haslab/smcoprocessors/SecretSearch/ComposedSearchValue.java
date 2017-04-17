package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.And;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Or;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

public class ComposedSearchValue extends AbstractSearchValue {

	static final Log LOG = LogFactory.getLog(ComposedSearchValue.class
			.getName());

	private final SearchCondition val1;
	private final SearchCondition val2;

	public ComposedSearchValue(Condition condition, SearchCondition val1,
			SearchCondition val2, int targetPlayer) {
		// TODO: check if condition is on the same column;
		super(condition, targetPlayer);
		this.val1 = val1;
		this.val2 = val2;
	}

	public List<Boolean> evaluateCondition(List<byte[]> value,
			List<byte[]> rowID, SharemindPlayer p) {
		LOG.debug("Going to do a composedSearchValue for condition "
				+ condition);
		List<Boolean> res1s = val1.evaluateCondition(value, rowID, p);
		List<Boolean> res2s = val2.evaluateCondition(value, rowID, p);
		List<Boolean> result = new ArrayList<Boolean>();

		for (int i = 0; i < res1s.size(); i++) {
			Boolean res1 = res1s.get(i);
			Boolean res2 = res2s.get(i);
			Boolean res = Boolean.TRUE;

			if (condition == And) {
				res = res1 && res2;
			} else if (condition == Or) {
				res = res1 || res2;
			}
			result.add(res);
		}
		return result;

	}

	public Condition getCondition() {
		return this.condition;
	}

}
