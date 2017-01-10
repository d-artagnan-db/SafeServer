package pt.uminho.haslab.smcoprocessors.SecretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Not;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

public class UnarySearchValue extends AbstractSearchValue {
	static final Log LOG = LogFactory.getLog(UnarySearchValue.class.getName());

	private final SearchCondition searchCondition;

	public UnarySearchValue(Condition condition,
			SearchCondition searchCondition, int targetPlayer) {
		// TODO: check if condition is on the same column;
		super(condition, targetPlayer);
		this.searchCondition = searchCondition;
	}

	@Override
	public boolean evaluateCondition(byte[] value, byte[] rowID,
			SharemindPlayer p) {
		System.out.println("Going to evaluate unary search value");
		if (condition == Not) {
			return !searchCondition.evaluateCondition(value, rowID, p);
		}

		return false;
	}

	public Condition getCondition() {
		return searchCondition.getCondition();
	}
}
