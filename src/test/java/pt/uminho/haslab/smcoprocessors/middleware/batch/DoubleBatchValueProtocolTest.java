package pt.uminho.haslab.smcoprocessors.middleware.batch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.TestLinkedRegions;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public abstract class DoubleBatchValueProtocolTest extends TestLinkedRegions {

	protected final List<Integer> nbits;
	protected final List<List<BigInteger>> valuesOne;
	protected final List<List<BigInteger>> valuesTwo;

	protected final Map<Integer, List<List<byte[]>>> secretsOne;
	protected final Map<Integer, List<List<byte[]>>> secretsTwo;
	protected final Map<Integer, List<List<byte[]>>> protocolResults;

	protected final Map<Integer, List<TestPlayer>> players;

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.TwoBatchValuesGenerator();
	}

	public DoubleBatchValueProtocolTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {

		super();

		this.nbits = nbits;
		this.valuesOne = valuesOne;
		this.valuesTwo = valuesTwo;

		secretsOne = new ConcurrentHashMap<Integer, List<List<byte[]>>>();
		secretsTwo = new ConcurrentHashMap<Integer, List<List<byte[]>>>();
		protocolResults = new ConcurrentHashMap<Integer, List<List<byte[]>>>();
		players = new ConcurrentHashMap<Integer, List<TestPlayer>>();

		for (int i = 0; i < nbits.size(); i++) {

			if (i == 0) {
				secretsOne.put(0, new ArrayList<List<byte[]>>());
				secretsOne.put(1, new ArrayList<List<byte[]>>());
				secretsOne.put(2, new ArrayList<List<byte[]>>());

				secretsTwo.put(0, new ArrayList<List<byte[]>>());
				secretsTwo.put(1, new ArrayList<List<byte[]>>());
				secretsTwo.put(2, new ArrayList<List<byte[]>>());

				protocolResults.put(0, new ArrayList<List<byte[]>>());
				protocolResults.put(1, new ArrayList<List<byte[]>>());
				protocolResults.put(2, new ArrayList<List<byte[]>>());

				players.put(0, new ArrayList<TestPlayer>());
				players.put(1, new ArrayList<TestPlayer>());
				players.put(2, new ArrayList<TestPlayer>());
			}

			List<byte[]> secretsOneU1 = new ArrayList<byte[]>();
			List<byte[]> secretsOneU2 = new ArrayList<byte[]>();
			List<byte[]> secretsOneU3 = new ArrayList<byte[]>();

			List<byte[]> secretsTwoU1 = new ArrayList<byte[]>();
			List<byte[]> secretsTwoU2 = new ArrayList<byte[]>();
			List<byte[]> secretsTwoU3 = new ArrayList<byte[]>();

			Dealer dealer = new SharemindDealer(nbits.get(i));

			for (int j = 0; j < valuesOne.get(i).size(); j++) {
				BigInteger valueOne = valuesOne.get(i).get(j);
				BigInteger valueTwo = valuesTwo.get(i).get(j);
				SharemindSharedSecret secretOne = (SharemindSharedSecret) dealer
						.share(valueOne);
				SharemindSharedSecret secretTwo = (SharemindSharedSecret) dealer
						.share(valueTwo);

				secretsOneU1.add(secretOne.getU1().toByteArray());
				secretsOneU2.add(secretOne.getU2().toByteArray());
				secretsOneU3.add(secretOne.getU3().toByteArray());

				secretsTwoU1.add(secretTwo.getU1().toByteArray());
				secretsTwoU2.add(secretTwo.getU2().toByteArray());
				secretsTwoU3.add(secretTwo.getU3().toByteArray());

			}
			secretsOne.get(0).add(secretsOneU1);
			secretsOne.get(1).add(secretsOneU2);
			secretsOne.get(2).add(secretsOneU3);

			secretsTwo.get(0).add(secretsTwoU1);
			secretsTwo.get(1).add(secretsTwoU2);
			secretsTwo.get(2).add(secretsTwoU3);

		}

		/*
		 * for(int i = 0; i < nbits.size(); i++){
		 * System.out.println("valuesOne "+valuesOne.get(i).size());
		 * System.out.println("valuesTwo "+valuesTwo.get(i).size());
		 * 
		 * System.out.println("secretsOne " + secretsOne.get(0).get(i).size());
		 * System.out.println("secretsTwo " + secretsOne.get(1).get(i).size());
		 * System.out.println("secretsThree " +
		 * secretsOne.get(2).get(i).size());
		 * 
		 * System.out.println("secretsTwoOne "+
		 * secretsTwo.get(0).get(i).size());
		 * System.out.println("secretsTwoTwo "+
		 * secretsTwo.get(1).get(i).size());
		 * System.out.println("secretsTwoThree " +
		 * secretsTwo.get(2).get(i).size());
		 * 
		 * 
		 * }
		 */

	}

	protected abstract List<byte[]> testingProtocol(
			List<byte[]> originalSecrets, List<byte[]> cmpSecrets);

	private class RSImpl extends TestRegionServer {

		public RSImpl(int playerID) throws IOException {
			super(playerID);
		}

		private BigInteger getMod(int nbits) {
			return BigInteger.valueOf(2).pow(nbits + 1);
		}

		@Override
		public void doComputation() {
			byte[] reqID = "1".getBytes();
			byte[] regionID = "1".getBytes();
			RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
			TestPlayer player = new TestPlayer(relay, ident, playerID, broker);
			players.get(playerID).add(player);

			for (int i = 0; i < nbits.size(); i++) {
				int valueNbits = nbits.get(i);

				List<byte[]> secretOne = secretsOne.get(playerID).get(i);
				List<byte[]> secretTwo = secretsTwo.get(playerID).get(i);

				SharemindSecretFunctions ssf = new SharemindSecretFunctions(
						valueNbits);

				// Comapare The values
				List<byte[]> res = testingProtocol(secretOne, secretTwo);
				protocolResults.get(playerID).add(res);

			}
			/**
			 * broker can only be cleaned after executing the protocol for every
			 * value or messages can be lost between values
			 */
			broker.allBatchMessagesRead(ident);

		}

	}

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {

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
