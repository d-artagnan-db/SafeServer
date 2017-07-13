package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public abstract class DoubleValueProtocolTest extends TestLinkedRegions {
	private static final Log LOG = LogFactory
			.getLog(DoubleValueProtocolTest.class.getName());

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.TwoValuesGenerator();
	}
	protected final List<Integer> nbits;
	protected final List<BigInteger> valuesOne;
	protected final List<BigInteger> valuesTwo;
	protected final Map<Integer, List<BigInteger>> secretsOne;
	protected final Map<Integer, List<BigInteger>> secretsTwo;
	protected final Map<Integer, List<BigInteger>> protocolResults;

	protected final Map<Integer, List<TestPlayer>> players;

	public DoubleValueProtocolTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {

		super();
		this.nbits = nbits;
		this.valuesOne = valuesOne;
		this.valuesTwo = valuesTwo;
		secretsOne = new ConcurrentHashMap<Integer, List<BigInteger>>();
		secretsTwo = new ConcurrentHashMap<Integer, List<BigInteger>>();
		protocolResults = new ConcurrentHashMap<Integer, List<BigInteger>>();
		players = new ConcurrentHashMap<Integer, List<TestPlayer>>();

		for (int i = 0; i < nbits.size(); i++) {
			if (i == 0) {
				secretsOne.put(0, new ArrayList<BigInteger>());
				secretsOne.put(1, new ArrayList<BigInteger>());
				secretsOne.put(2, new ArrayList<BigInteger>());

				secretsTwo.put(0, new ArrayList<BigInteger>());
				secretsTwo.put(1, new ArrayList<BigInteger>());
				secretsTwo.put(2, new ArrayList<BigInteger>());

				protocolResults.put(0, new ArrayList<BigInteger>());
				protocolResults.put(1, new ArrayList<BigInteger>());
				protocolResults.put(2, new ArrayList<BigInteger>());

				players.put(0, new ArrayList<TestPlayer>());
				players.put(1, new ArrayList<TestPlayer>());
				players.put(2, new ArrayList<TestPlayer>());
			}

			Dealer dealer = new SharemindDealer(nbits.get(i));
			BigInteger valueOne = valuesOne.get(i);
			BigInteger valueTwo = valuesTwo.get(i);
			SharemindSharedSecret secretOne = (SharemindSharedSecret) dealer
					.share(valueOne);
			secretsOne.get(0).add(secretOne.getU1());
			secretsOne.get(1).add(secretOne.getU2());
			secretsOne.get(2).add(secretOne.getU3());

			SharemindSharedSecret secretTwo = (SharemindSharedSecret) dealer
					.share(valueTwo);

			secretsTwo.get(0).add(secretTwo.getU1());
			secretsTwo.get(1).add(secretTwo.getU2());
			secretsTwo.get(2).add(secretTwo.getU3());
		}

	}

	private class RSImpl extends TestRegionServer {

		public RSImpl(int playerID) throws IOException {
			super(playerID);
		}

		private BigInteger getMod(int nbits) {
			return BigInteger.valueOf(2).pow(nbits + 1);
		}

		private SharemindSecret generateSecret(int nbits, BigInteger value,
				Player player) throws InvalidSecretValue {
			return new SharemindSecret(nbits + 1, getMod(nbits), value, player);
		}

		@Override
		public void doComputation() {
			byte[] reqID = "1".getBytes();
			byte[] regionID = "1".getBytes();
			RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
			TestPlayer player = new TestPlayer(relay, ident, playerID, broker);
			players.get(playerID).add(player);

			for (int i = 0; i < nbits.size(); i++) {
				try {
					int valueNbits = nbits.get(i);

					BigInteger secretOne = secretsOne.get(playerID).get(i);
					BigInteger secretTwo = secretsTwo.get(playerID).get(i);

					// Create a secret for each value
					Secret originalSecret = generateSecret(valueNbits,
							secretOne, player);
					Secret cmpSecret = generateSecret(valueNbits, secretTwo,
							player);
					// Comapare The values
					SharemindSecret secret = testingProtocol(originalSecret,
							cmpSecret);
					protocolResults.get(playerID).add(secret.getValue());
					// Tell the broker that every message has been read for this
					// request.

				} catch (InvalidSecretValue ex) {
					throw new IllegalStateException(ex);
				}
			}
			/**
			 * broker can only be cleaned after executing the protocol for every
			 * value or messages can be lost between values
			 */
			broker.allBatchMessagesRead(ident);

		}

	}

	protected abstract void validateResults() throws InvalidSecretValue;

	protected abstract SharemindSecret testingProtocol(Secret originalSecret,
			Secret cmpSecret);

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

	public void printMessageCount() {
		for (Integer playerID : players.keySet()) {

			for (TestPlayer p : players.get(playerID)) {

				for (Integer i : p.getMessagesSent().keySet()) {
					LOG.debug(playerID + " sent to " + i + ", "
							+ p.getMessagesSent().get(i).size() + " messages");
				}

				for (Integer j : p.getMessagesReceived().keySet()) {
					LOG.debug(playerID + " received "
							+ p.getMessagesReceived().get(j).size() + " from "
							+ j);
				}

			}
		}
	}

	public void validatePlayersMessagesOrder() {

		for (Integer playerID : players.keySet()) {

			for (int i = 0; i < players.get(playerID).size(); i++) {
				TestPlayer p = players.get(playerID).get(i);

				for (Integer playerDest : p.getMessagesSent().keySet()) {

					assertEquals(p.getMessagesSent().get(playerDest).size(),
							players.get(playerDest).get(i)
									.getMessagesReceived().get(playerID).size());
					for (int j = 0; j < p.getMessagesSent().get(playerDest)
							.size(); j++) {

						BigInteger sentVal = p.getMessagesSent()
								.get(playerDest).get(j);
						BigInteger recVal = players.get(playerDest).get(i)
								.getMessagesReceived().get(playerID).get(j);

						assertEquals(sentVal, recVal);

					}

				}
			}
		}
	}

}
