package pt.uminho.haslab.smcoprocessors.middleware;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;
import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;

@RunWith(Parameterized.class)
public class ShareConvProtocolTest extends TestLinkedRegions {

	private final Map<Integer, List<BigInteger>> values;
	private final List<Integer> nbits;
	private final Map<Integer, List<BigInteger>> protocolResults;

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.ShareConvGenerator();
	}

	public ShareConvProtocolTest(List<Integer> nbits,
			List<BigInteger> valuesOne, List<BigInteger> valuesTwo,
			List<BigInteger> valuesThree) throws IOException,
			InvalidNumberOfBits, InvalidSecretValue {

		super();
		this.nbits = nbits;
		this.values = new ConcurrentHashMap<Integer, List<BigInteger>>();
		this.values.put(0, valuesOne);
		this.values.put(1, valuesTwo);
		this.values.put(2, valuesThree);

		protocolResults = new ConcurrentHashMap<Integer, List<BigInteger>>();

		protocolResults.put(0, new ArrayList<BigInteger>());

		protocolResults.put(1, new ArrayList<BigInteger>());

		protocolResults.put(2, new ArrayList<BigInteger>());

	}

	@Override
	protected void validateResults() throws InvalidSecretValue {
		for (int i = 0; i < nbits.size(); i++) {

			BigInteger valueOne = values.get(0).get(i);
			BigInteger valueTwo = values.get(1).get(i);
			BigInteger valueThree = values.get(2).get(i);
			SharemindSharedSecret result = new SharemindSharedSecret(1,
					protocolResults.get(0).get(i), protocolResults.get(1)
							.get(i), protocolResults.get(2).get(i));

			assertEquals(result.unshare(),
					valueOne.xor(valueTwo).xor(valueThree));
		}
	}

	private BigInteger getMod(int nbits) {
		return BigInteger.valueOf(2).pow(nbits + 1);
	}

	protected SharemindSecret testingProtocol(Secret originalSecret) {
		return (SharemindSecret) ((SharemindSecret) originalSecret).shareConv();
	}

	private class RSImpl extends TestRegionServer {

		public RSImpl(int playerID) throws IOException {
			super(playerID);
		}
		private SharemindSecret generateSecret(int nbits, BigInteger value,
				ContextPlayer player) throws InvalidSecretValue {
			return new SharemindSecret(nbits + 1, getMod(nbits), value, player);
		}
		@Override
		public void doComputation() {
			byte[] reqID = "1".getBytes();
			byte[] regionID = "1".getBytes();
			RequestIdentifier ident = new RequestIdentifier(reqID, regionID);

			ContextPlayer player = new ContextPlayer(relay, ident, playerID,
					broker);

			for (int i = 0; i < nbits.size(); i++) {

				try {
					int valueNbits = nbits.get(i);

					BigInteger secret = values.get(playerID).get(i);

					// Create a secret for each value
					Secret secretValue = generateSecret(valueNbits, secret,
							player);

					SharemindSecret shareSecret = testingProtocol(secretValue);
					protocolResults.get(playerID).add(shareSecret.getValue());
					// Tell the broker that every message has been read for this
					// request.
				} catch (InvalidSecretValue ex) {
					throw new IllegalStateException(ex);
				}
			}
		}

	}
	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

}
