package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.protocolresults.IntPlayerResults;
import pt.uminho.haslab.saferegions.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.sharemindImp.Integer.IntSharemindSecretFunctions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;


public class IntSearchValue extends SearchValue {

    private static final IntSharemindSecretFunctions ssf = new IntSharemindSecretFunctions();

    private static final Map<String, Map<String, int[]>> cacheValues = new HashMap<String, Map<String, int[]>>();
    private static final Lock cacheDataLock = new ReentrantLock();

    private final String column;
    private final String regionIdentifier;

    public IntSearchValue(int nBits, List<byte[]> value, Condition condition, SmpcConfiguration config, String column, String regionIdent) {
        super(nBits, value, condition, config);
        this.column = column;
        this.regionIdentifier = regionIdent;
    }

    public int[] convertInts(List<byte[]> value) {

        ///Key should contain local player ID for local tests.
        //String key = player.getPlayerID() + ":"  + column;
        String key = column;
        if (config.isCachedData() && cacheValues.containsKey(key) && cacheValues.get(key).containsKey(regionIdentifier)) {
            return cacheValues.get(key).get(regionIdentifier);
        } else {
            int[] vals = new int[value.size()];

            for (int i = 0; i < value.size(); i++) {
                vals[i] = ByteBuffer.wrap(value.get(i)).getInt();
            }

            if (config.isCachedData()) {
                //This causes unit tests to fail because the three clusters see the same values as the cache is static.
                cacheDataLock.lock();
                if (!cacheValues.containsKey(key)) {
                    cacheValues.put(key, new HashMap<String, int[]>());
                }

                if (!cacheValues.get(key).containsKey(regionIdentifier)) {
                    cacheValues.get(key).put(regionIdentifier, vals);
                }
                cacheDataLock.unlock();

            }
            return vals;
        }
    }

    public int[] convertInt(byte[] value) {
        int[] vals = new int[1];
        vals[0] = ByteBuffer.wrap(value).getInt();
        return vals;
    }

    public void evaluateCondition(List<byte[]> cmpValues, List<byte[]> rowIDs,
                                  SharemindPlayer player) {

        List<Boolean> fIndex;
        try {
            int[] result;
            int[] values;
            int[] intCmpValues;
            /* *
             * Batch protocol comparison protocols require that the array of
             * values being compared have the same size. As the value to be
             * compared is always the same, it is created a list with the same
             * size as the values being compared.
             *
             * e.g: Values = [val1, val1, val1] cmpValues = [val2, val3, val4]
             * In this example val1 is compared to every other value.
             */

            values = convertInt(value.get(0));
            intCmpValues = convertInts(cmpValues);


            if (condition == Equal) {
                result = ssf.equal(values, intCmpValues, player);
            } else {
                result = ssf.greaterOrEqualThan(intCmpValues, values, player);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(cmpValues.size() + " input result size is " + result.length);
            }

            if (player.isTargetPlayer()) {
                // At this point the size of the list identifiers must be 2.
                List<int[]> results = player.getIntProtocolResults();

                //LOG.debug("Received results " + Arrays.toString(result));
                results.add(result);
                IntPlayerResults playerResults = new IntPlayerResults(results, condition, nBits);
                fIndex = playerResults.declassify();
                /**
                 * if no matching element was found, an empty list is sent. When
                 * the other players receives an empty list, it knows that no
                 * index was found.
                 */
                int[] toSend = new int[fIndex.size()];
                for (int i = 0; i < fIndex.size(); i++) {
                    Boolean b = fIndex.get(i);
                    toSend[i] = b ? 1 : 0;

                    // Prepare the results to be send to the other players.
                    byte[] rowID = rowIDs.get(i);
                    resultIndex.put(new String(rowID), b);
                    resultsList.add(b);

                }
                player.sendFilteredIndexes(toSend);

            } else {
                player.sendIntProtocolResults(result);
                int[] res = player.getFilterIndexes();

                for (int i = 0; i < res.length; i++) {
                    int val = res[i];
                    byte[] rowID = rowIDs.get(i);
                    Boolean decRes = val == 1 ? Boolean.TRUE : Boolean.FALSE;
                    String key = new String(rowID);
                    resultIndex.put(key, decRes);
                    resultsList.add(decRes);
                }
            }
        } catch (ResultsLengthMismatch | ResultsIdentifiersMismatch | InvalidSecretValue ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }
}
