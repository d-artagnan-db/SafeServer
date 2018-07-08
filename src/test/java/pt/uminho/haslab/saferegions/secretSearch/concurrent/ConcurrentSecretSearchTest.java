package pt.uminho.haslab.saferegions.secretSearch.concurrent;

import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.helpers.ConcurrentBatchProtocolTest;
import pt.uminho.haslab.saferegions.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.TestCase.assertEquals;

public abstract class ConcurrentSecretSearchTest
        extends
        ConcurrentBatchProtocolTest {

    private final Map<Integer, Map<Integer, List<Boolean>>> results;

    public ConcurrentSecretSearchTest(List<Integer> nbits,
                                      List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);

        results = new ConcurrentHashMap<Integer, Map<Integer, List<Boolean>>>();

        for (int i = 0; i < 3; i++) {
            results.put(i, new ConcurrentHashMap<Integer, List<Boolean>>());
        }

    }

    @Override
    protected ConcurrentBatchTestPlayer createConcurrentPlayer(Relay relay,
                                                               RequestIdentifier requestID, int playerID, MessageBroker broker,
                                                               List<byte[]> firstValueSecret, List<byte[]> secondValueSecret,
                                                               int nBits) {
        return new SecretSearchBatchTestPlayer(relay, requestID, playerID,
                broker, firstValueSecret, secondValueSecret, nBits);
    }

    @Override
    protected void validateResults() throws InvalidSecretValue {

        for (Integer request : results.get(0).keySet()) {
            List<Boolean> ressOne = results.get(0).get(request);
            List<Boolean> ressTwo = results.get(1).get(request);
            List<Boolean> ressThree = results.get(2).get(request);

            List<Boolean> expectedValues = getSearchExpectedResult(request);

            for (int j = 0; j < ressOne.size(); j++) {
                boolean expected = expectedValues.get(j);
                boolean resOne = ressOne.get(j);
                boolean resTwo = ressTwo.get(j);
                boolean resThree = ressThree.get(j);
                assertEquals(resOne, resTwo);
                assertEquals(resTwo, resThree);
                assertEquals(expected, resOne);

            }
        }
    }

    protected abstract SearchCondition getSearchCondition(int nBits,
                                                          List<byte[]> firstValueSecret);

    protected abstract List<Boolean> getSearchExpectedResult(Integer request);

    protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected class SecretSearchBatchTestPlayer
            extends
            ConcurrentBatchTestPlayer {

        private List<Boolean> searchRes;

        public SecretSearchBatchTestPlayer(Relay relay,
                                           RequestIdentifier requestID, int playerID,
                                           MessageBroker broker, List<byte[]> firstVals,
                                           List<byte[]> secondVals, int nBits) {
            super(relay, requestID, playerID, broker, firstVals, secondVals,
                    nBits);
        }

        @Override
        public void run() {
            Integer reqID = Integer.parseInt(new String(requestID
                    .getRequestID()));

            BigInteger rowID = BigInteger.valueOf(reqID);

            player.setTargetPlayer(1);

            /**
             * Simulation of comparison of values inside hbase scan. The second
             * value is stored on the hbase the database. The first value is
             * request by the user to compare to the values stored in the db.
             * The comparisons should be made as follows: secretTwo == secretOne
             * secretTwo >= secretOne secretTwo > secretOne secretTwo <
             * secretOne secretTwo <= SecretOne
             */

            SearchCondition condition = getSearchCondition(nBits,
                    secondValueSecret);
            List<byte[]> ids = new ArrayList<byte[]>();
            for (int i = 0; i < firstValueSecret.size(); i++) {
                ids.add(rowID.toByteArray());
            }
            condition.evaluateCondition(firstValueSecret, ids, player);

            searchRes = condition.getClassificationList();
            Integer playerID = player.getPlayerID();
            results.get(playerID).put(reqID, searchRes);
            player.cleanResultsMatch();

        }

        public List<Boolean> getSearchRes() {
            return searchRes;
        }

        @Override
        protected List<byte[]> testingProtocol(List<byte[]> firstValueSecret,
                                               List<byte[]> secondValueSecret) {
            throw new UnsupportedOperationException("Not supported yet.");
        }


    }
}
