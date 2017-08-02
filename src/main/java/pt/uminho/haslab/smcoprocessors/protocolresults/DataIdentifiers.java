package pt.uminho.haslab.smcoprocessors.protocolresults;

import java.math.BigInteger;
import java.util.List;

/**
 * Class used to hold the results from a single party after executing a protocol
 * on multiple database entries.
 */
public class DataIdentifiers {

    /* All the secrets for each entry */
    private final List<BigInteger> secrets;
    /* The id of each entry */
    private final List<BigInteger> identifiers;

    public DataIdentifiers(List<BigInteger> secrets,
                           List<BigInteger> identifiers) throws ResultsLengthMissmatch {
        if (secrets.size() != identifiers.size()) {
            throw new ResultsLengthMissmatch();
        }
        this.secrets = secrets;
        this.identifiers = identifiers;
    }

    public List<BigInteger> getSecrets() {
        return secrets;
    }

    public List<BigInteger> getIdentifiers() {
        return identifiers;
    }

}
