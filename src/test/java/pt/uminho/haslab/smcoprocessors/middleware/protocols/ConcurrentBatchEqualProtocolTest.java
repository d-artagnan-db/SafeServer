package pt.uminho.haslab.smcoprocessors.middleware.protocols;

import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchProtocolTest;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

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
