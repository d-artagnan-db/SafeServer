package pt.uminho.haslab.saferegions.protocols;

import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.helpers.BatchProtocolTest;
import pt.uminho.haslab.saferegions.helpers.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class BatchGreaterOrEqualThan extends BatchProtocolTest {
	public BatchGreaterOrEqualThan(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);
	}

	protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
		int comparisonResult = valOne.compareTo(valtwo);
		return comparisonResult == 0 || comparisonResult == 1 ? 0 : 1;
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
			try {
				return ssf.greaterOrEqualThan(secretOne, secretTwo, player);
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
