package pt.uminho.haslab.smcoprocessors.protocolresults;

public class ResultsIdentifiersMissmatch extends Exception {

    /**
     * Creates a new instance of <code>ResultsIdentifiersMissmatch</code>
     * without detail message.
     */
    public ResultsIdentifiersMissmatch() {
    }

    /**
     * Constructs an instance of <code>ResultsIdentifiersMissmatch</code> with
     * the specified detail message.
     *
     * @param msg the detail message.
     */
    public ResultsIdentifiersMissmatch(String msg) {
        super(msg);
    }
}
