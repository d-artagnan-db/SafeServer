package pt.uminho.haslab.smcoprocessors.SecretSearch;

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
		Equal, GreaterOrEqualThan, And, Or, Not
	}

	/**
	 * 
	 * @param value
	 * @param rowID
	 * @param p
	 * @return
	 */
	public boolean evaluateCondition(byte[] value, byte[] rowID,
			SharemindPlayer p);

	public Condition getCondition();
}
