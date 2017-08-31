package pt.uminho.haslab.smcoprocessors.middleware.protocols;

import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchProtocolTest;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ConcurrentBatchEqualProtocolTest
        extends
        ConcurrentBatchProtocolTest {

    public ConcurrentBatchEqualProtocolTest(List<Integer> nbits,
                                            List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
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

    @Override
    protected void validateResults() throws InvalidSecretValue {

        for (int i = 0; i < nbits.size(); i++) {

            RSImpl firstRS = (RSImpl) getRegionServer(0);
            RSImpl secondRS = (RSImpl) getRegionServer(1);
            RSImpl thirdRS = (RSImpl) getRegionServer(2);

            for (int j = 0; j < valuesOne.get(i).size(); j++) {
                BigInteger fVal = new BigInteger((firstRS).getResult(i).get(j));
                BigInteger sVal = new BigInteger((secondRS).getResult(i).get(j));
                BigInteger tVal = new BigInteger((thirdRS).getResult(i).get(j));

                SharemindSharedSecret result = new SharemindSharedSecret(1,
                        fVal, sVal, tVal);

                boolean comparisonResult = valuesOne.get(i).get(j)
                        .equals(valuesTwo.get(i).get(j));
                int expectedResult = comparisonResult ? 1 : 0;
                assertEquals(result.unshare().intValue(), expectedResult);
            }
        }

    }

    @Override
    protected ConcurrentBatchTestPlayer createConcurrentPlayer(Relay relay,
                                                               RequestIdentifier requestID, int playerID, MessageBroker broker,
                                                               List<byte[]> firstValueSecret, List<byte[]> secondValueSecret,
                                                               int nBits) {
        return new EqualConcurrentPlayer(relay, requestID, playerID, broker,
                firstValueSecret, secondValueSecret, nBits);
    }

}
