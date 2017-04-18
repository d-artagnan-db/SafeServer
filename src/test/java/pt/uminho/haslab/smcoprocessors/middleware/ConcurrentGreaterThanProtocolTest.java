package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentTestPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

public class ConcurrentGreaterThanProtocolTest extends ConcurrentProtocolTest {

	public ConcurrentGreaterThanProtocolTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	private class GreaterThanConcurrentPlayer extends ConcurrentTestPlayer {

		public GreaterThanConcurrentPlayer(Relay relay,
				RequestIdentifier requestID, int playerID,
				MessageBroker broker, BigInteger firstValueSecret,
				BigInteger secondValueSecret, int nBits) {
			super(relay, requestID, playerID, broker, firstValueSecret,
					secondValueSecret, nBits);
		}

		@Override
		protected SharemindSecret testingProtocol(Secret originalSecret,
				Secret cmpSecret) {
			return (SharemindSecret) originalSecret
					.greaterOrEqualThan(cmpSecret);
		}

	}

	@Override
	protected ConcurrentTestPlayer createConcurrentPlayer(Relay relay,
			RequestIdentifier requestID, int playerID, MessageBroker broker,
			BigInteger firstValueSecret, BigInteger secondValueSecret, int nBits) {
		return new GreaterThanConcurrentPlayer(relay, requestID, playerID,
				broker, firstValueSecret, secondValueSecret, nBits);
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {
		for (int i = 0; i < nbits.size(); i++) {

			RSImpl firstRS = (RSImpl) getRegionServer(0);
			RSImpl secondRS = (RSImpl) getRegionServer(1);
			RSImpl thirdRS = (RSImpl) getRegionServer(2);

			BigInteger fVal = (firstRS).getResult(i);
			BigInteger sVal = (secondRS).getResult(i);
			BigInteger tVal = (thirdRS).getResult(i);

			SharemindSharedSecret result = new SharemindSharedSecret(1, fVal,
					sVal, tVal);

			int comparisonResult = valuesOne.get(i).compareTo(valuesTwo.get(i));
			int expectedResult = comparisonResult == 0 || comparisonResult == 1
					? 0
					: 1;
			assertEquals(expectedResult, result.unshare().intValue());
		}
	}

}
