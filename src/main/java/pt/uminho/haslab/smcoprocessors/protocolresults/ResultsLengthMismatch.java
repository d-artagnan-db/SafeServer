package pt.uminho.haslab.smcoprocessors.protocolresults;

public class ResultsLengthMismatch extends Exception {

	/**
	 * Creates a new instance of <code>ResultsLenghtDontMatch</code> without
	 * detail message.
	 */
	public ResultsLengthMismatch() {
	}

	/**
	 * Constructs an instance of <code>ResultsLenghtDontMatch</code> with the
	 * specified detail message.
	 * 
	 * @param msg
	 *            the detail message.
	 */
	public ResultsLengthMismatch(String msg) {
		super(msg);
	}
}
