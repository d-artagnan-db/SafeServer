package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import static junit.framework.TestCase.assertEquals;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;

public abstract class SecretSearchTest extends DoubleValueProtocolTest {

	private static final Log LOG = LogFactory.getLog(SecretSearchTest.class
			.getName());
	public SecretSearchTest(List<Integer> nbits, List<BigInteger> valuesOne,
			List<BigInteger> valuesTwo) throws IOException,
			InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	protected abstract SearchCondition getSearchCondition(int valueNBits,
			byte[] value, int targetPlayer);

	private class RSImpl extends TestRegionServer {

		public RSImpl(int playerID) throws IOException {
			super(playerID);
		}

		@Override
		public void doComputation() {
			byte[] reqID = "1".getBytes();
			byte[] regionID = "1".getBytes();
			RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
			TestPlayer player = new TestPlayer(relay, ident, playerID, broker);
			if (this.playerID == 1) {
				player.setTargetPlayer();
			}

			players.get(playerID).add(player);

			for (int i = 0; i < nbits.size(); i++) {

				int valueNbits = nbits.get(i);

				BigInteger secretOne = secretsOne.get(playerID).get(i);
				BigInteger secretTwo = secretsTwo.get(playerID).get(i);

				/**
				 * Simulation of comparison of values inside hbase scan. The
				 * second value is stored on the hbase the database. The first
				 * value is request by the user to compare to the values stored
				 * in the db. The comparison should be made as follows:
				 * secretTwo == secretOne secretTwo >= secretOne secretTwo >
				 * secretOne secretTwo < secretOne secretTwo <= SecretOne
				 */
				SearchCondition cond = getSearchCondition(valueNbits + 1,
						secretTwo.toByteArray(), 1);

				boolean searchRes = cond.evaluateCondition(
						secretOne.toByteArray(), reqID, player);
				LOG.debug("Expected result " + searchRes);
				BigInteger result = BigInteger.ZERO;

				if (searchRes) {
					result = BigInteger.ONE;
				}

				protocolResults.get(playerID).add(result);

			}
			/**
			 * broker can only be cleaned after executing the protocol for every
			 * value or messages can be lost between values
			 */
			broker.allMessagesRead(ident);

		}

	}

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {

		for (int i = 0; i < nbits.size(); i++) {

			// The results stored from all of the parties should be the same
			LOG.debug(protocolResults.get(0));
			LOG.debug(protocolResults.get(1));
			LOG.debug(protocolResults.get(2));
			assertEquals(protocolResults.get(0), protocolResults.get(1));
			assertEquals(protocolResults.get(1), protocolResults.get(2));

			int expectedResult = compareOriginalValues(valuesOne.get(i),
					valuesTwo.get(i));
			assertEquals(expectedResult, protocolResults.get(0).get(i)
					.intValue());
		}
	}

	protected abstract int compareOriginalValues(BigInteger first,
			BigInteger second);

	@Override
	protected SharemindSecret testingProtocol(Secret originalSecret,
			Secret cmpSecret) {
		throw new UnsupportedOperationException(
				"Operation not used in this test");
	}

}
