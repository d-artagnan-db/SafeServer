package pt.uminho.haslab.smcoprocessors.protocolresults;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used to hold the results from a single party after executing a protocol
 * on multiple database entries. Contains multiple results of an mpc batch operation.Similar to DataIdentifiers.
 */
public class SearchResults {

    /* All the secrets for each entry */
    private final List<byte[]> secrets;
    /* The id of each entry */
    private final List<byte[]> identifiers;

    public SearchResults(List<byte[]> secrets, List<byte[]> identifiers)
            throws ResultsLengthMissmatch {
        if (secrets.size() != identifiers.size()) {
            throw new ResultsLengthMissmatch();
        }
        this.secrets = secrets;
        this.identifiers = identifiers;
    }

    public List<byte[]> getSecrets() {
        return secrets;
    }

    public List<byte[]> getIdentifiers() {
        return identifiers;
    }

    public void printSize() {
        System.out.println("Secrets " + secrets.size() + " ident "
                + identifiers.size());
    }

    public DataIdentifiers toDataIdentifier() throws ResultsLengthMissmatch {
        List<BigInteger> bSecrets = new ArrayList<BigInteger>();
        List<BigInteger> bIdentifiers = new ArrayList<BigInteger>();

        for (byte[] secret : secrets) {
            bSecrets.add(new BigInteger(secret));
        }

        for (byte[] identifier : identifiers) {
            bIdentifiers.add(new BigInteger(identifier));
        }

        return new DataIdentifiers(bSecrets, bIdentifiers);

    }

    public void printALl(int playerID) {
        System.out.println(playerID + " printing all searchResults");
        for (int i = 0; i < secrets.size(); i++) {
            String ident = new String(identifiers.get(i));
            BigInteger secret = new BigInteger(secrets.get(i));
            System.out.println("pres " + playerID + " " + ident + " - "
                    + secret);

        }
    }

}
