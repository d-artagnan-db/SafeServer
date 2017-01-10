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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentTestPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public abstract class ConcurrentProtocolTest extends TestLinkedRegions {

	private static final Log LOG = LogFactory
			.getLog(ConcurrentProtocolTest.class.getName());

	protected final List<Integer> nbits;
	protected final List<BigInteger> valuesOne;
	protected final List<BigInteger> valuesTwo;
	protected final Map<Integer, List<BigInteger>> secretsOne;
	protected final Map<Integer, List<BigInteger>> secretsTwo;

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.TwoValuesGenerator();
	}

	public ConcurrentProtocolTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {

		super();
		this.nbits = nbits;
		this.valuesOne = valuesOne;
		this.valuesTwo = valuesTwo;
		secretsOne = new ConcurrentHashMap<Integer, List<BigInteger>>();
		secretsTwo = new ConcurrentHashMap<Integer, List<BigInteger>>();

		for (int i = 0; i < nbits.size(); i++) {
			if (i == 0) {
				secretsOne.put(0, new ArrayList<BigInteger>());
				secretsOne.put(1, new ArrayList<BigInteger>());
				secretsOne.put(2, new ArrayList<BigInteger>());

				secretsTwo.put(0, new ArrayList<BigInteger>());
				secretsTwo.put(1, new ArrayList<BigInteger>());
				secretsTwo.put(2, new ArrayList<BigInteger>());

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

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID, secretsOne.get(playerID),
				secretsTwo.get(playerID), nbits);
	}

	protected abstract ConcurrentTestPlayer createConcurrentPlayer(Relay relay,
			RequestIdentifier requestID, int playerID, MessageBroker broker,
			BigInteger firstValueSecret, BigInteger secondValueSecret, int nBits);

	protected class RSImpl extends TestRegionServer {

		private final int playerID;

		private final List<BigInteger> firstValueSecrets;

		private final List<BigInteger> secondValueSecrets;

		private final List<ConcurrentTestPlayer> requests;

		public RSImpl(int playerID, List<BigInteger> firstValueSecrets,
				List<BigInteger> secondValueSecrets, List<Integer> nbits)
				throws IOException {
			super(playerID);

			this.playerID = playerID;
			this.firstValueSecrets = firstValueSecrets;
			this.secondValueSecrets = secondValueSecrets;
			this.requests = new ArrayList<ConcurrentTestPlayer>();

		}

		public BigInteger getResult(int index) {
			return requests.get(index).getResultSecret().getValue();
		}

		@Override
		public void doComputation() {

			for (int i = 0; i < nbits.size(); i++) {
				int valueNbits = nbits.get(i);

				BigInteger secretOne = firstValueSecrets.get(i);
				BigInteger secretTwo = secondValueSecrets.get(i);
				// System.out.println("Nbits size "+i);

				byte[] reqID = ("" + i).getBytes();
				byte[] regionID = "1".getBytes();
				RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
				ConcurrentTestPlayer player = createConcurrentPlayer(relay,
						ident, playerID, broker, secretOne, secretTwo,
						valueNbits);

				requests.add(player);
			}

			for (ConcurrentTestPlayer p : requests) {
				p.start();
			}

			for (ConcurrentTestPlayer p : requests) {
				try {
					p.join();
				} catch (InterruptedException ex) {
					LOG.debug(ex);
					throw new IllegalStateException(ex);
				}
			}
		}
	}
}
