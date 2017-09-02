package pt.uminho.haslab.smcoprocessors.middleware.protocols;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


public class ConcurrentBatchGreaterOrEqualThanProtocolTest extends
        ConcurrentBatchProtocolTest {

    private static final Log LOG = LogFactory
            .getLog(ConcurrentBatchGreaterOrEqualThanProtocolTest.class.getName());

    public ConcurrentBatchGreaterOrEqualThanProtocolTest(List<Integer> nbits, List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo) throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    protected ConcurrentBatchTestPlayer createConcurrentPlayer(Relay relay, RequestIdentifier requestID, int playerID, MessageBroker broker, List<byte[]> firstValueSecret, List<byte[]> secondValueSecret, int nBits) {
        return new ConcurrentPlayerImpl(relay, requestID, playerID, broker,
                firstValueSecret, secondValueSecret, nBits);
    }

    protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
        int comparisonResult = valOne.compareTo(valtwo);
        return comparisonResult == 0 || comparisonResult == 1 ? 0 : 1;
    }

    private class ConcurrentPlayerImpl extends ConcurrentBatchTestPlayer {

        public ConcurrentPlayerImpl(Relay relay, RequestIdentifier requestID,
                                    int playerID, MessageBroker broker, List<byte[]> firstVals,
                                    List<byte[]> secondVals, int nBits) {
            super(relay, requestID, playerID, broker, firstVals, secondVals,
                    nBits);
        }

        @Override
        protected List<byte[]> testingProtocol(List<byte[]> firstValueSecret,
                                               List<byte[]> secondValueSecret) {
            SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits);
            try {
                return ssf.greaterOrEqualThan(firstValueSecret, secondValueSecret, this);
            } catch (InvalidNumberOfBits invalidNumberOfBits) {
                LOG.debug(invalidNumberOfBits);
                throw new IllegalStateException(invalidNumberOfBits);
            } catch (InvalidSecretValue invalidSecretValue) {
                LOG.debug(invalidSecretValue);
                throw new IllegalStateException(invalidSecretValue);
            }
        }
    }


}
