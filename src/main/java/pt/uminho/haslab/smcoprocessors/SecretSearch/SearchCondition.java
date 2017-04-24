package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.util.List;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

public interface SearchCondition {

	/**
	 * The smpc library only supports by default the Equal and
	 * GreaterOrEquanThan protocols.
	 * 
	 * The other comparison can be obtained by combining those two, the
	 * following way: GreaterThan = !Equal && GreaterOrEqualThan LesserThan =
	 * !GreaterOrEqualThan LesserOrEqualThan = !GreaterOrEqualThan && Equal
	 */
	public enum Condition {
		Equal, GreaterOrEqualThan, Greater, Less, LessOrEqualThan, NotEqual, And, Or, Not, Nop
	}

	/**
	 * Executes the comparison of a batch of values with a smpc. An empty list
	 * is returned if no match is found or a list of ids that satisfy the
	 * protocol.
	 * 
	 * @param value
	 * @param rowID
	 * @param p
	 * @return
	 */
	public List<Boolean> evaluateCondition(List<byte[]> value,
			List<byte[]> rowID, SharemindPlayer p);

	public Condition getCondition();
}
