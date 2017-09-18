package pt.uminho.haslab.smcoprocessors.protocolresults;

public class ResultsIdentifiersMismatch extends Exception {

    /**
     * Creates a new instance of <code>ResultsIdentifiersMismatch</code> without
     * detail message.
     */
    public ResultsIdentifiersMismatch() {
    }

    /**
     * Constructs an instance of <code>ResultsIdentifiersMismatch</code> with
     * the specified detail message.
     *
     * @param msg the detail message.
     */
    public ResultsIdentifiersMismatch(String msg) {
        super(msg);
    }
}
