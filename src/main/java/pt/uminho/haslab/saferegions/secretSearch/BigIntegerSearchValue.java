package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.protocolresults.PlayerResults;
import pt.uminho.haslab.saferegions.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSecretFunctions;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;

public class BigIntegerSearchValue extends SearchValue {

    public BigIntegerSearchValue(int nBits, List<byte[]> value, Condition condition, SmpcConfiguration config) {
        super(nBits, value, condition, config);
    }

    public void evaluateCondition(List<byte[]> cmpValues, List<byte[]> rowIDs,
                                  SharemindPlayer player) {

        List<Boolean> fIndex;
        try {
            List<byte[]> result;

            SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits);

            List<byte[]> values = new ArrayList<byte[]>();

            /* *
             * Batch protocol comparison protocols require that the array of
             * values being compared have the same size. As the value to be
             * compared is always the same, it is created a list with the same
             * size as the values being compared.
             *
             * e.g: Values = [val1, val1, val1] cmpValues = [val2, val3, val4]
             * In this example val1 is compared to every other value.
             */

            if (value.size() == 1 && cmpValues.size() > 1) {
                // If there is only a single value replicate it
                for (int i = 0; i < cmpValues.size(); i++) {
                    values.add(value.get(0));
                }
            } else if (value.size() == cmpValues.size()) {
                values = value;
            } else {
                throw new IllegalStateException(
                        "The size of values list being compared is invalid");
            }


            if (condition == Equal) {
                result = ssf.equal(values, cmpValues, player);
            } else {
                result = ssf.greaterOrEqualThan(cmpValues, values, player);
            }

            if (player.isTargetPlayer()) {

                // At this point the size of the list identifiers must be 2.
                List<List<byte[]>> results = player.getProtocolResults();

                results.add(result);
                PlayerResults playerResults = new PlayerResults(results, condition, nBits);
                fIndex = playerResults.declassify();
                /**
                 * if no matching element was found, an empty list is sent. When
                 * the other players receives an empty list, it knows that no
                 * index was found.
                 */
                int[] toSend = new int[fIndex.size()];

                for (int i = 0; i < fIndex.size(); i++) {
                    Boolean b = fIndex.get(i);
                    byte[] rowID = rowIDs.get(i);
                    toSend[i] = b ? 1 : 0;
                    // Prepare the results to be send to the other players.
                    resultIndex.put(new String(rowID), b);
                    resultsList.add(b);

                }

                player.sendFilteredIndexes(toSend);

            } else {

                player.sendProtocolResults(result);
                int[] res = player.getFilterIndexes();

                for (int i = 0; i < res.length; i++) {
                    int val = res[i];
                    byte[] rowID = rowIDs.get(i);
                    Boolean decRes = val == 1 ? Boolean.TRUE : Boolean.FALSE;
                    resultIndex.put(new String(rowID), decRes);
                    resultsList.add(decRes);
                }
            }
        } catch (InvalidSecretValue | ResultsLengthMismatch | InvalidNumberOfBits | ResultsIdentifiersMismatch ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }
}
