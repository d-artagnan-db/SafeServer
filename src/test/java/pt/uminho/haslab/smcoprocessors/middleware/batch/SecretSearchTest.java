package pt.uminho.haslab.smcoprocessors.middleware.batch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public abstract class SecretSearchTest extends DoubleBatchValueProtocolTest {

	private static final Log LOG = LogFactory.getLog(SecretSearchTest.class);

	private final Map<Integer, List<List<Boolean>>> boolResults;

	public SecretSearchTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
		boolResults = new HashMap<Integer, List<List<Boolean>>>();
	}

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

	protected abstract SearchCondition getSearchCondition(int valueNBits,
			List<byte[]> value, int targetPlayer);

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

				List<byte[]> secretOne = secretsOne.get(playerID).get(i);
				List<byte[]> secretTwo = secretsTwo.get(playerID).get(i);

				/**
				 * Simulation of comparison of values inside hbase scan. The
				 * second value is stored on the hbase the database. The first
				 * value is request by the user to compare to the values stored
				 * in the db. The comparison should be made as follows:
				 * secretTwo == secretOne secretTwo >= secretOne secretTwo >
				 * secretOne secretTwo < secretOne secretTwo <= SecretOne
				 */
				SearchCondition cond = getSearchCondition(valueNbits + 1,
						secretTwo, 1);
				List<byte[]> bIds = new ArrayList<byte[]>();

				for (byte[] set : secretOne) {
					bIds.add(reqID);
				}

				boolean searchRes;
				List<Boolean> lbRes = cond.evaluateCondition(secretOne, bIds,
						player);
				searchRes = lbRes.get(0);
				// System.out.println("1-Expected result " + lbRes.size());
				LOG.debug("Expected result " + searchRes);
				// System.out.println("Bools "+
				// boolResults.containsKey(player.getPlayerID()));
				if (!boolResults.containsKey(player.getPlayerID())) {
					// System.out.println("2-BoolResults inside size is "+
					// boolResults.size());
					boolResults.put(player.getPlayerID(),
							new ArrayList<List<Boolean>>());
				}

				boolResults.get(playerID).add(lbRes);
				// System.out.println("BoolResults inside size is "+
				// boolResults.size());

			}
			/**
			 * broker can only be cleaned after executing the protocol for every
			 * value or messages can be lost between values
			 */
			broker.allBatchMessagesRead(ident);

		}

	}
	@Override
	protected void validateResults() throws InvalidSecretValue {

		for (int i = 0; i < nbits.size(); i++) {

			// The results stored from all of the parties should be the same
			// LOG.debug(protocolResults.get(0));
			// LOG.debug(protocolResults.get(1));
			// LOG.debug(protocolResults.get(2));
			for (int j = 0; j < boolResults.get(0).get(i).size(); j++) {
				assertEquals(boolResults.get(0).get(i).get(j),
						boolResults.get(1).get(i).get(j));
				assertEquals(boolResults.get(1).get(i).get(j),
						boolResults.get(2).get(i).get(j));

				// System.out.println(valuesOne.get(i)+" <-> "+valuesTwo.get(i)+
				// " = "+protocolResults.get(0).get(i)
				// .intValue());

				boolean expectedRes = compareOriginalValues(valuesOne.get(i)
						.get(j), valuesTwo.get(i).get(j));
				// System.out.println("Expected "+ expectedRes + " <-> " +
				// boolResults.get(0).get(i).get(j).booleanValue());

				assertEquals(expectedRes, boolResults.get(0).get(i).get(j)
						.booleanValue());
			}

		}

	}
	protected abstract boolean compareOriginalValues(BigInteger first,
			BigInteger second);

}
