package pt.uminho.haslab.smcoprocessors.SecretSearch;

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

	public boolean evaluateCondition(byte[] value, byte[] rowID,
			SharemindPlayer p) {
		LOG.debug("Going to do a composedSearchValue for condition "
				+ condition);
		boolean res1 = val1.evaluateCondition(value, rowID, p);
		boolean res2 = val2.evaluateCondition(value, rowID, p);

		if (condition == And) {
			return res1 && res2;
		} else if (condition == Or) {
			return res1 || res2;
		}
		return false;

	}

	public Condition getCondition() {
		return this.condition;
	}

}
