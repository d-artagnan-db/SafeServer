package pt.uminho.haslab.saferegions.protocols;

import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.helpers.ConcurrentBatchProtocolTest;
import pt.uminho.haslab.saferegions.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSecretFunctions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class ConcurrentBatchEqualProtocolTest
        extends
        ConcurrentBatchProtocolTest {

    public ConcurrentBatchEqualProtocolTest(List<Integer> nbits,
                                            List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected ConcurrentBatchTestPlayer createConcurrentPlayer(Relay relay,
                                                               RequestIdentifier requestID, int playerID, MessageBroker broker,
                                                               List<byte[]> firstValueSecret, List<byte[]> secondValueSecret,
                                                               int nBits) {
        return new EqualConcurrentPlayer(relay, requestID, playerID, broker,
                firstValueSecret, secondValueSecret, nBits);
    }

    protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
        boolean comparisonResult = valOne.equals(valtwo);
        return comparisonResult ? 1 : 0;
    }

    private class EqualConcurrentPlayer extends ConcurrentBatchTestPlayer {

        public EqualConcurrentPlayer(Relay relay, RequestIdentifier requestID,
                                     int playerID, MessageBroker broker, List<byte[]> firstVals,
                                     List<byte[]> secondVals, int nBits) {
            super(relay, requestID, playerID, broker, firstVals, secondVals,
                    nBits);
        }

        @Override
        protected List<byte[]> testingProtocol(List<byte[]> firstValueSecret,
                                               List<byte[]> secondValueSecret) {
            SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits);
            return ssf.equal(firstValueSecret, secondValueSecret, this);
        }

    }

}
