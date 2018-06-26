package pt.uminho.haslab.saferegions.protocolresults;

import pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSharedSecret;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class PlayerResults {

    private final Condition condition;
    private final int nBits;
    private final List<List<byte[]>> results;

    /**
     * We are assuming that the class is created correctly with 3 lists inside the results list. One for each player.
     **/
    public PlayerResults(List<List<byte[]>> results, Condition condition,
                         int nBits) throws ResultsLengthMismatch {

        this.condition = condition;
        this.results = results;
        this.nBits = nBits;
    }

    /**
     * Iterates through the results of the smpc protocols and declassifies the
     * result. The function returns True for row keys that satisfy the
     * protocols.
     *
     * @return BigInteger with the corresponding Index
     * @throws ResultsIdentifiersMismatch
     */
    public List<Boolean> declassify() throws ResultsIdentifiersMismatch {

        List<Boolean> resultIDS = new ArrayList<Boolean>();

        for (int i = 0; i < results.get(0).size(); i++) {

            byte[] bFirstSecret = results.get(0).get(i);
            byte[] bSecondSecret = results.get(1).get(i);
            byte[] bThirdSecret = results.get(2).get(i);

            BigInteger firstSecret = new BigInteger(bFirstSecret);
            BigInteger secondSecret = new BigInteger(bSecondSecret);
            BigInteger thirdSecret = new BigInteger(bThirdSecret);


            if (condition == Equal) {
                SharemindSharedSecret secretResult = new SharemindSharedSecret(
                        1, firstSecret, secondSecret, thirdSecret);

                int result = secretResult.unshare().intValue();

                if (result == 1) {
                    resultIDS.add(Boolean.TRUE);
                } else {
                    resultIDS.add(Boolean.FALSE);
                }

            } else if (condition == GreaterOrEqualThan) {
                SharemindSharedSecret secretResult = new SharemindSharedSecret(
                        nBits + 1, firstSecret, secondSecret, thirdSecret);

                int result = secretResult.unshare().intValue();

                if (result == 0) {
                    resultIDS.add(Boolean.TRUE);
                } else {
                    resultIDS.add(Boolean.FALSE);
                }
            }
        }

        return resultIDS;
    }
}
