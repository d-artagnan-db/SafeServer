package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.protocolresults.*;
import pt.uminho.haslab.smpc.sharemindImp.IntSharemindSecretFunctions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;


public class IntSearchValue  extends SearchValue{

    public IntSearchValue(int nBits, List<byte[]> value, Condition condition) {
        super(nBits, value, condition);
    }


    public int[] convertInts(List<byte[]> value){
       int[] vals = new int[value.size()];
        //ByteBuffer buffer = ByteBuffer.allocate(4* value.size());

        /*for(byte[] val: value){
            buffer.put(val);
        }*/
        //buffer.flip();
        for(int i = 0; i < value.size(); i++){
            vals[i] = ByteBuffer.wrap(value.get(i)).getInt();//buffer.getInt(i);
        }

        //buffer.clear();
        return vals;
    }


    public int[] duplicateInt(byte[] value, int nTimes){
        int[] vals = new int[nTimes];
        //buffer.put(value);
        //buffer.flip();

        for(int i = 0; i < nTimes; i++){
            vals[i] = ByteBuffer.wrap(value).getInt();//buffer.getInt();
        }
       // buffer.clear();
        return vals;
    }
    public void evaluateCondition(List<byte[]> cmpValues, List<byte[]> rowIDs,
                                  SharemindPlayer player) {

        List<Boolean> fIndex;
        try {
            int[] result;

            IntSharemindSecretFunctions ssf = new IntSharemindSecretFunctions();

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
			LOG.debug("Going to generate values");

            if (value.size() == 1 && cmpValues.size() > 1) {
                // If there is only a single value replicate it
                values  = duplicateInt(value.get(0), cmpValues.size());
                intCmpValues = convertInts(cmpValues);
            } else if (value.size() == cmpValues.size()) {
                values = convertInts(value);
                intCmpValues = convertInts(cmpValues);
            } else {
                throw new IllegalStateException(
                        "The size of values list being compared is invalid");
            }


            if (LOG.isDebugEnabled()) {
                LOG.debug("Running protocol " + condition);
            }

           // LOG.debug(player.getPlayerID()+ " protocol input values are  " + Arrays.toString(values) + " and stored values are "+ Arrays.toString(intCmpValues));
            if (condition == Equal) {
                result = ssf.equal(values, intCmpValues, player);
            } else {
                throw new IllegalStateException("Operation not yet supported");
            }
            //LOG.debug(player.getPlayerID()+ " has result " + Arrays.toString(result));

            List<Integer> protoResults = new ArrayList<Integer>(result.length);
            for(int val: result){
                protoResults.add(val);
            }

            if (player.isTargetPlayer()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrieve protocol results from peers");
                }
                // At this point the size of the list identifiers must be 2.
                List<List<Integer>> results = player.getIntProtocolResults();

                results.add(protoResults);
                IntPlayerResults playerResults = new IntPlayerResults(results, condition, nBits);
                fIndex = playerResults.declassify();
                /**
                 * if no matching element was found, an empty list is sent. When
                 * the other players receives an empty list, it knows that no
                 * index was found.
                 */
                List<byte[]> toSend = new ArrayList<byte[]>();

                for (int i = 0; i < fIndex.size(); i++) {
                    Boolean b = fIndex.get(i);
                    byte[] rowID = rowIDs.get(i);
                    toSend.add(b.toString().getBytes());
                    // Prepare the results to be send to the other players.
                    resultIndex.put(new BigInteger(rowID), b);
                    resultsList.add(b);

                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Send filter results to peers");
                }
                FilteredIndexes filtIndex = new FilteredIndexes(toSend);
                player.sendFilteredIndexes(filtIndex);

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("end protocol results to target");
                }
                player.sendIntProtocolResults(result);
                List<byte[]> res = player.getFilterIndexes().getIndexes();

                for (int i = 0; i < res.size(); i++) {
                    byte[] val = res.get(i);
                    byte[] rowID = rowIDs.get(i);
                    Boolean decRes = Boolean.parseBoolean(new String(val));
                    resultIndex.put(new BigInteger(rowID), decRes);
                    resultsList.add(decRes);
                }
            }
        } catch (ResultsLengthMismatch | ResultsIdentifiersMismatch ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }
}
