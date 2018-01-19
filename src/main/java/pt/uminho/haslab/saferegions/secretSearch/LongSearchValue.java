package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.protocolresults.LongPlayerResults;
import pt.uminho.haslab.saferegions.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.sharemindImp.Long.LongSharemindSecretFunctions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;

public class LongSearchValue extends SearchValue {

    private long[] cacheValues;

    public LongSearchValue(int nBits, List<byte[]> value, SearchCondition.Condition condition, SmpcConfiguration configuration) {
        super(nBits, value, condition, configuration);
    }


    public long[] convertLongs(List<byte[]> value) {
        if (config.isCachedData() && cacheValues != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Computing over cached data");
            }
            return cacheValues;
        } else {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Computing over loaded data");
            }

            long[] vals = new long[value.size()];

            for (int i = 0; i < value.size(); i++) {
                vals[i] = ByteBuffer.wrap(value.get(i)).getLong();
            }

            if (config.isCachedData()) {
                cacheValues = vals;
            }

            return vals;
        }
    }


    /*public long[] duplicateLongs(byte[] value, int nTimes) {

        long[] vals = new long[nTimes];
        for (int i = 0; i < nTimes; i++) {
            vals[i] = ByteBuffer.wrap(value).getLong();
        }
        return vals;
    }*/

    public long[] convertLong(byte[] value) {
        long[] vals = new long[1];
        vals[0] = ByteBuffer.wrap(value).getLong();
        return vals;
    }

    public void evaluateCondition(List<byte[]> cmpValues, List<byte[]> rowIDs,
                                  SharemindPlayer player) {

        List<Boolean> fIndex;
        try {
            long[] result;

            LongSharemindSecretFunctions ssf = new LongSharemindSecretFunctions();

            long[] values;
            long[] intCmpValues;
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
                values = convertLong(value.get(0));
                intCmpValues = convertLongs(cmpValues);
            } else if (value.size() == cmpValues.size()) {
                values = convertLongs(value);
                intCmpValues = convertLongs(cmpValues);
            } else {
                throw new IllegalStateException(
                        "The size of values list being compared is invalid");
            }


            if (LOG.isDebugEnabled()) {
                LOG.debug("Running protocol " + condition + " with value " + values.length + " and cmpValues " + intCmpValues.length);
            }

            if (condition == Equal) {
                result = ssf.equal(values, intCmpValues, player);
            } else {
                result = ssf.greaterOrEqualThan(intCmpValues, values, player);
            }

            if (player.isTargetPlayer()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrieve protocol results from peers");
                }
                // At this point the size of the list identifiers must be 2.
                List<long[]> results = player.getLongProtocolResults();
                results.add(result);

                LongPlayerResults playerResults = new LongPlayerResults(results, condition, nBits);
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
                    //toSend.add(b.toString().getBytes());
                    // Prepare the results to be send to the other players.
                    resultIndex.put(new BigInteger(rowID), b);
                    resultsList.add(b);

                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Send filter results to peers");
                }
                player.sendFilteredIndexes(toSend);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("end protocol results to target");
                }
                player.sendLongProtocolResults(result);
                int[] res = player.getFilterIndexes();

                for (int i = 0; i < res.length; i++) {
                    int val = res[i];
                    byte[] rowID = rowIDs.get(i);
                    Boolean decRes = val == 1 ? Boolean.TRUE : Boolean.FALSE;
                    resultIndex.put(new BigInteger(rowID), decRes);
                    resultsList.add(decRes);
                }
            }
        } catch (ResultsLengthMismatch | ResultsIdentifiersMismatch | InvalidSecretValue ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }
}
