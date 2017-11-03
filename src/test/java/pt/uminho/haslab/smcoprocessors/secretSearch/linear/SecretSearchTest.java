package pt.uminho.haslab.smcoprocessors.secretSearch.linear;

import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.helpers.BatchProtocolTest;
import pt.uminho.haslab.smcoprocessors.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public abstract class SecretSearchTest extends BatchProtocolTest {

	private final Map<Integer, Map<Integer, List<Boolean>>> results;

	public SecretSearchTest(List<Integer> nbits,
			List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
			throws IOException, InvalidNumberOfBits, InvalidSecretValue {
		super(nbits, valuesOne, valuesTwo);

		results = new HashMap<Integer, Map<Integer, List<Boolean>>>();

		for (int i = 0; i < 3; i++) {
			results.put(i, new HashMap<Integer, List<Boolean>>());
		}
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {
		for (Integer request : results.get(0).keySet()) {
			List<Boolean> ressOne = results.get(0).get(request);
			List<Boolean> ressTwo = results.get(1).get(request);
			List<Boolean> ressThree = results.get(2).get(request);

			List<Boolean> expectedValues = getSearchExpectedResult(request);

			for (int j = 0; j < ressOne.size(); j++) {
				boolean expected = expectedValues.get(j);
				boolean resOne = ressOne.get(j);
				boolean resTwo = ressTwo.get(j);
				boolean resThree = ressThree.get(j);
				assertEquals(resOne, resTwo);
				assertEquals(resTwo, resThree);
				assertEquals(expected, resOne);

			}
		}
	}

	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RegionServerImpl(playerID, secretsOne.get(playerID),
				secretsTwo.get(playerID), nbits);
	}

	protected abstract List<Boolean> getSearchExpectedResult(Integer request);

	protected abstract SearchCondition getSearchCondition(int nBits,
			List<byte[]> firstValueSecret);

	protected int getExpectedResult(BigInteger valOne, BigInteger valtwo) {
		throw new UnsupportedOperationException("Not supported yet.");
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
			SharemindPlayer splayer = (SharemindPlayer) player;
			Integer reqID = Integer.parseInt(new String(ident.getRequestID()));
			BigInteger rowID = BigInteger.valueOf(reqID);

			splayer.setTargetPlayer(1);

			/**
			 * Simulation of comparison of values inside hbase scan. The second
			 * value is stored on the hbase the database. The first value is
			 * request by the user to compare to the values stored in the db.
			 * The comparisons should be made as follows: secretTwo == secretOne
			 * secretTwo >= secretOne secretTwo > secretOne secretTwo <
			 * secretOne secretTwo <= SecretOne
			 */
			SearchCondition condition = getSearchCondition(nBits, secretTwo);

			List<byte[]> ids = new ArrayList<byte[]>();
			for (int i = 0; i < secretOne.size(); i++) {
				ids.add(rowID.toByteArray());
			}

			condition.evaluateCondition(secretOne, ids, splayer);

			List<Boolean> searchRes = condition.getClassificationList();

			Integer playerId = player.getPlayerID();
			results.get(playerId).put(reqID, searchRes);
			splayer.cleanResultsMatch();
			// This result should not be used by classes that iextend the
			// SecretSearchTest
			return null;
		}
	}

}
