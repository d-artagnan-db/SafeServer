package pt.uminho.haslab.smcoprocessors.protocols;

import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.helpers.BatchProtocolTest;
import pt.uminho.haslab.smcoprocessors.helpers.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class BatchEqualProtocolTest extends BatchProtocolTest {

	public BatchEqualProtocolTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
		boolean comparisonResult = valOne.equals(valtwo);
		return comparisonResult ? 1 : 0;
	}

	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RegionServerImpl(playerID, secretsOne.get(playerID),
				secretsTwo.get(playerID), nbits);
	}

	private class RegionServerImpl extends ProtoRegionServer {

		public RegionServerImpl(int playerID,
				List<List<byte[]>> firstValueSecrets,
				List<List<byte[]>> secondValueSecrets, List<Integer> nbits)
				throws IOException {
			super(playerID, firstValueSecrets, secondValueSecrets, nbits);
		}

		public List<byte[]> executeProtocol(Player player,
				List<byte[]> secretOne, List<byte[]> secretTwo, int nBits,
				RequestIdentifier ident) {
			SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits);
			return ssf.equal(secretOne, secretTwo, player);
		}
	}

}
