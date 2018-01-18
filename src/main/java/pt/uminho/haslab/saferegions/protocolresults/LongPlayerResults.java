package pt.uminho.haslab.saferegions.protocolresults;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.smpc.sharemindImp.Long.LongSharemindDealer;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class LongPlayerResults {

    private static final Log LOG = LogFactory.getLog(IntPlayerResults.class
            .getName());
    private final SearchCondition.Condition condition;
    private final List<List<Long>> results;

    /**
     * We are assuming that the class is created correctly with 3 lists inside the results list. One for each player.
     **/
    public LongPlayerResults(List<List<Long>> results, SearchCondition.Condition condition,
                             int nBits) throws ResultsLengthMismatch {

        this.condition = condition;
        this.results = results;
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

            Long bFirstSecret = results.get(0).get(i);
            Long bSecondSecret = results.get(1).get(i);
            Long bThirdSecret = results.get(2).get(i);

            long[] secrets = new long[3];
            secrets[0] = bFirstSecret;
            secrets[1] = bSecondSecret;
            secrets[2] = bThirdSecret;
            LongSharemindDealer dealer = new LongSharemindDealer();

            if (condition == Equal) {
                long result = dealer.unshareBit(secrets);
                if (result == 1) {
                    resultIDS.add(Boolean.TRUE);
                } else {
                    resultIDS.add(Boolean.FALSE);
                }

            } else if (condition == GreaterOrEqualThan) {
                long result = dealer.unshare(secrets);

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
