package pt.uminho.haslab.smcoprocessors.middleware.secretSearch;

import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchProtocolTest;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

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
        return new SecretSearchBatchTestPlayer(relay, requestID, playerID, broker,
                firstValueSecret, secondValueSecret, nBits);
    }

    @Override
    protected void validateResults() throws InvalidSecretValue {

        for (Integer request : results.get(0).keySet()) {
            List<Boolean> ressOne = results.get(0).get(request);
            List<Boolean> ressTwo = results.get(1).get(request);
            List<Boolean> ressThree = results.get(2).get(request);

            List<Boolean> expecteds = getSearchExpectedResult(request);

            for (int j = 0; j < ressOne.size(); j++) {
                boolean expected = expecteds.get(j);
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
                                                          List<byte[]> firstValueSecret, int i);

    protected abstract List<Boolean> getSearchExpectedResult(Integer request);

    protected class SecretSearchBatchTestPlayer extends ConcurrentBatchTestPlayer {

        private List<Boolean> searchRes;

        public SecretSearchBatchTestPlayer(Relay relay, RequestIdentifier requestID,
                                           int playerID, MessageBroker broker, List<byte[]> firstVals,
                                           List<byte[]> secondVals, int nBits) {
            super(relay, requestID, playerID, broker, firstVals, secondVals,
                    nBits);
        }

        @Override
        public void run() {
            Integer reqID = Integer.parseInt(new String(requestID
                    .getRequestID()));

            BigInteger rowID = BigInteger.valueOf(reqID);

            if (player.getPlayerID() == 1) {
                player.setTargetPlayer();
            }

            /**
             * Simulation of comparison of values inside hbase scan. The second
             * value is stored on the hbase the database. The first value is
             * request by the user to compare to the values stored in the db.
             * The comparison should be made as follows: secretTwo == secretOne
             * secretTwo >= secretOne secretTwo > secretOne secretTwo <
             * secretOne secretTwo <= SecretOne
             */
            SearchCondition condition = getSearchCondition(nBits,
                    secondValueSecret, 1);
            List<byte[]> ids = new ArrayList<byte[]>();
            for (int i = 0; i < firstValueSecret.size(); i++) {
                ids.add(rowID.toByteArray());
            }
            searchRes = condition.evaluateCondition(firstValueSecret, ids,
                    player);
            Integer playerID = player.getPlayerID();
            results.get(playerID).put(reqID, searchRes);

        }

        public List<Boolean> getSearchRes() {
            return searchRes;
        }

        @Override
        protected List<byte[]> testingProtocol(List<byte[]> firstValueSecret,
                                               List<byte[]> secondValueSecret) {
            throw new UnsupportedOperationException("Not supported yet."); // To
        }
    }
}
