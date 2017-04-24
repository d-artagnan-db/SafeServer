package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static junit.framework.TestCase.assertEquals;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentTestPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;

public abstract class ConcurrentSecretSearchTest extends ConcurrentProtocolTest {
	private static final Log LOG = LogFactory.getLog(ConcurrentTestPlayer.class
			.getName());

	private final Map<Integer, Map<Integer, Boolean>> results;

	public ConcurrentSecretSearchTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
		results = new ConcurrentHashMap<Integer, Map<Integer, Boolean>>();

		for (int i = 0; i < 3; i++) {
			results.put(i, new ConcurrentHashMap<Integer, Boolean>());
		}
	}

	protected abstract SearchCondition getSearchCondition(int nBits,
			byte[] firstValueSecret, int i);

	protected abstract boolean getSearchExpectedResult(Integer request);

	@Override
	protected ConcurrentTestPlayer createConcurrentPlayer(Relay relay,
			RequestIdentifier requestID, int playerID, MessageBroker broker,
			BigInteger firstValueSecret, BigInteger secondValueSecret, int nBits) {
		return new SecretSearchTestPlayer(relay, requestID, playerID, broker,
				firstValueSecret, secondValueSecret, nBits);
	}

	protected class SecretSearchTestPlayer extends ConcurrentTestPlayer {

		private boolean searchRes;

		public SecretSearchTestPlayer(Relay relay, RequestIdentifier requestID,
				int playerID, MessageBroker broker,
				BigInteger firstValueSecret, BigInteger secondValueSecret,
				int nBits) {
			super(relay, requestID, playerID, broker, firstValueSecret,
					secondValueSecret, nBits);
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
					secondValueSecret.toByteArray(), 1);
			List<byte[]> vals = new ArrayList<byte[]>();
			List<byte[]> ids = new ArrayList<byte[]>();
			vals.add(firstValueSecret.toByteArray());
			ids.add(rowID.toByteArray());
			searchRes = condition.evaluateCondition(vals, ids, player).get(0);
			Integer playerID = player.getPlayerID();
			results.get(playerID).put(reqID, searchRes);

		}

		public boolean getSearchRes() {
			return searchRes;
		}

		@Override
		protected SharemindSecret testingProtocol(Secret originalSecret,
				Secret cmpSecret) {
			throw new UnsupportedOperationException("Not supported yet.");
		}
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {
		LOG.debug("going to validate results");
		for (Integer request : results.get(0).keySet()) {

			boolean resOne = results.get(0).get(request);
			boolean resTwo = results.get(1).get(request);
			boolean resThree = results.get(2).get(request);

			boolean expected = getSearchExpectedResult(request);

			assertEquals(resOne, resTwo);
			assertEquals(resTwo, resThree);
			assertEquals(expected, resOne);

		}
	}

}
