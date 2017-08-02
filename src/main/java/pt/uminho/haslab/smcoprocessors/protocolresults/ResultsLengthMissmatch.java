package pt.uminho.haslab.smcoprocessors.protocolresults;

public class ResultsLengthMissmatch extends Exception {

    /**
     * Creates a new instance of <code>ResultsLenghtDontMatch</code> without
     * detail message.
     */
    public ResultsLengthMissmatch() {
    }

    /**
     * Constructs an instance of <code>ResultsLenghtDontMatch</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public ResultsLengthMissmatch(String msg) {
        super(msg);
    }
}
