package pt.uminho.haslab.saferegions.protocolresults;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition;
import pt.uminho.haslab.saferegions.secureRegionScanner.HandleSafeFilter;
import pt.uminho.haslab.smpc.sharemindImp.IntSharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.SharemindSharedSecret;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class IntPlayerResults {

    private static final Log LOG = LogFactory.getLog(IntPlayerResults.class
            .getName());
    private final Condition condition;
    private final int nBits;
    private final List<List<Integer>> results;

    /**
     * We are assuming that the class is created correctly with 3 lists inside the results list. One for each player.
     **/
    public IntPlayerResults(List<List<Integer>> results, Condition condition,
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

            Integer bFirstSecret = results.get(0).get(i);
            Integer bSecondSecret = results.get(1).get(i);
            Integer bThirdSecret = results.get(2).get(i);

            int[] secrets = new int[3];
            secrets[0] = bFirstSecret;
            secrets[1] = bSecondSecret;
            secrets[2] = bThirdSecret;
            LOG.debug("index " + i  + " protocol results are  " + Arrays.toString(secrets));
            if (condition == Equal) {
                IntSharemindDealer dealer = new IntSharemindDealer();
                int result = dealer.unshareBit(secrets);
                LOG.debug("Index " + i + " had result " + result);
                if (result == 1) {
                    resultIDS.add(Boolean.TRUE);
                } else {
                    resultIDS.add(Boolean.FALSE);
                }

            } else if (condition == GreaterOrEqualThan) {
               throw new IllegalStateException("Operation not supported yet.");
            }
        }

        return resultIDS;
    }
}
