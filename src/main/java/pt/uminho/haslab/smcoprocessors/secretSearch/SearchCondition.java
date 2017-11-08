package pt.uminho.haslab.smcoprocessors.secretSearch;

import java.util.List;

public interface SearchCondition {

	/**
	 * Executes the comparison of a protocols of values with a SMPC. An empty
	 * list is returned if no match is found or a list of ids that satisfy the
	 * protocol.
	 * 
	 * @param value
	 * @param rowID
	 * @param p
	 * @return
	 */
	void evaluateCondition(List<byte[]> value, List<byte[]> rowID,
			SharemindPlayer p);

	Condition getCondition();

	/***
	 * 
	 * This method should only be used after evaluateCondition. Once the
	 * condition was evaluated, then the SearchCondition implementation should
	 * hold a map of rowIDs to classification results. This method returns the
	 * result of a given rowID.
	 * 
	 * */
	boolean getRowClassification(byte[] row);

	/**
	 * This method clears any state that might be stored on a concrete
	 * implementation after the evaluateCondition method is used.
	 */
	void clearSearchIndexes();

	/**
	 * The SMPC library only supports by default the Equal and
     * GreaterOrEqualThan protocols.
     * <p>
	 * The other comparison can be obtained by combining those two, the
	 * following way: GreaterThan = !Equal && GreaterOrEqualThan LesserThan =
	 * !GreaterOrEqualThan LesserOrEqualThan = !GreaterOrEqualThan && Equal
	 */
	enum Condition {
		Equal, GreaterOrEqualThan, Greater, Less, LessOrEqualThan, NotEqual, And, Or, Not, Nop, Xor, NestedHandler, Scan
	}

	List<Boolean> getClassificationList();

}
