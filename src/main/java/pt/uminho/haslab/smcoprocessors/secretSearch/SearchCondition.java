package pt.uminho.haslab.smcoprocessors.secretSearch;

import java.util.List;

public interface SearchCondition {

	/**
	 * Executes the comparison of a protocols of values with a smpc. An empty
	 * list is returned if no match is found or a list of ids that satisfy the
	 * protocol.
	 * 
	 * @param value
	 * @param rowID
	 * @param p
	 * @return
	 */
	List<Boolean> evaluateCondition(List<byte[]> value, List<byte[]> rowID,
			SharemindPlayer p);

	Condition getCondition();

	/**
	 * The smpc library only supports by default the Equal and
	 * GreaterOrEquanThan protocols.
	 * <p>
	 * The other comparison can be obtained by combining those two, the
	 * following way: GreaterThan = !Equal && GreaterOrEqualThan LesserThan =
	 * !GreaterOrEqualThan LesserOrEqualThan = !GreaterOrEqualThan && Equal
	 */
	enum Condition {
		Equal, GreaterOrEqualThan, Greater, Less, LessOrEqualThan, NotEqual, And, Or, Not, Nop, Xor
	}

}
