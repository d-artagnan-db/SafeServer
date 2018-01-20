package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.protocolresults.IntPlayerResults;
import pt.uminho.haslab.saferegions.protocolresults.ResultsIdentifiersMismatch;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.sharemindImp.Integer.IntSharemindSecretFunctions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;


public class IntSearchValue extends SearchValue{

    private static final IntSharemindSecretFunctions ssf = new IntSharemindSecretFunctions();

    private static final Map<String,Map<BigInteger, int[]>> cacheValues = new HashMap<String, Map<BigInteger, int[]>>();
    private static final Lock cacheDataLock = new ReentrantLock();

    private final String column;
    private final BigInteger regionIdentifier;

    public IntSearchValue(int nBits, List<byte[]> value, Condition condition, SmpcConfiguration config, String column, BigInteger regionIdent) {
        super(nBits, value, condition, config);
        this.column = column;
        this.regionIdentifier = regionIdent;
    }

    public int[] convertInts(List<byte[]> value, SharemindPlayer player){

        ///Key should contain local player ID for local tests.
        //String key = player.getPlayerID() + ":"  + column;
        String key = column;
        if (config.isCachedData() && cacheValues.containsKey(key) && cacheValues.get(key).containsKey(regionIdentifier)) {
            /*if (LOG.isDebugEnabled()) {
                LOG.debug("Computing over cached data");
            }*/
            //LOG.info("Geting cahced values " +  key + "::"+regionIdentifier + "::"  + Arrays.toString(cacheValues.get(key).get(regionIdentifier)));
            return cacheValues.get(key).get(regionIdentifier);
        } else {
            /*if (LOG.isDebugEnabled()) {
                LOG.debug("Computing over loaded data");
            }*/

            int[] vals = new int[value.size()];

            for (int i = 0; i < value.size(); i++) {
                vals[i] = ByteBuffer.wrap(value.get(i)).getInt();
            }

            if (config.isCachedData()) {
                //This causes tests to fail because the three clusters see the same values as the cache is static.
                cacheDataLock.lock();
                if(!cacheValues.containsKey(key)){
                    cacheValues.put(key, new HashMap<BigInteger, int[]>());
                }

                if(!cacheValues.get(key).containsKey(regionIdentifier)){
                    //LOG.info("Storing cached values "+key + "::"+regionIdentifier + "vals " + Arrays.toString(vals));

                    cacheValues.get(key).put(regionIdentifier, vals);
                }
                cacheDataLock.unlock();

            }
            return vals;
        }
    }


    /*public int[] duplicateInt(byte[] value, int nTimes){
        int[] vals = new int[nTimes];
        for(int i = 0; i < nTimes; i++){
            vals[i] = ByteBuffer.wrap(value).getInt();
        }
        return vals;
    }*/

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
			//LOG.debug("Going to generate values");

            values = convertInt(value.get(0));
            intCmpValues = convertInts(cmpValues, player);


            /*if (LOG.isDebugEnabled()) {
                LOG.debug("Running protocol " + condition);
            }*/

            //LOG.debug("Input values " + Arrays.toString(values) + " <-> " + Arrays.toString(intCmpValues));
            if (condition == Equal) {
                //LOG.debug("Input values " + values.length + " <-> " + intCmpValues.length);
                result = ssf.equal(values, intCmpValues, player);
            } else {
                result = ssf.greaterOrEqualThan(intCmpValues, values, player);
            }

            //LOG.debug("Result is " + Arrays.toString(result));

            if (player.isTargetPlayer()) {
                /*if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrieve protocol results from peers");
                }*/
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
                    byte[] rowID = rowIDs.get(i);
                    toSend[i] = b ? 1 : 0;
                    // Prepare the results to be send to the other players.
                    resultIndex.put(new BigInteger(rowID), b);
                    resultsList.add(b);

                }
                /*if (LOG.isDebugEnabled()) {
                    LOG.debug("Send filter results to peers");
                }*/
                player.sendFilteredIndexes(toSend);

            } else {
                /*if (LOG.isDebugEnabled()) {
                    LOG.debug("end protocol results to target");
                }*/
                //LOG.debug("Sending result " + Arrays.toString(result));
                player.sendIntProtocolResults(result);
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
