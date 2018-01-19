package pt.uminho.haslab.saferegions.protocols;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.helpers.RegionServer;
import pt.uminho.haslab.saferegions.helpers.TestLinkedRegions;
import pt.uminho.haslab.saferegions.helpers.TestRegionServer;
import pt.uminho.haslab.saferegions.protocolresults.FilteredIndexes;
import pt.uminho.haslab.saferegions.secretSearch.ContextPlayer;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class FilterIndexesTest extends TestLinkedRegions {

	private final List<Integer> playerSourceIDs;

	private final List<BigInteger> indexes;

	private final Map<Integer, Queue<FilteredIndexes>> recIndexes;

	public FilterIndexesTest(List<Integer> playerSourceIDs,
			List<BigInteger> indexes) throws IOException {
		this.playerSourceIDs = playerSourceIDs;
		this.indexes = indexes;

		recIndexes = new ConcurrentHashMap<Integer, Queue<FilteredIndexes>>();

		for (int i = 0; i < 3; i++) {
			recIndexes.put(i, new LinkedList<FilteredIndexes>());
		}
	}

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.FilterIndexesGenerator();
	}

	@Override
	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new RSImpl(playerID);
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {

		for (int i = 0; i < indexes.size(); i++) {
			int playerSource = playerSourceIDs.get(i);

			int[] playersDest = getDestPlayer(playerSource);

			for (int playerDest : playersDest) {

				FilteredIndexes recIndex = recIndexes.get(playerDest).poll();
				byte[] recIndexBs = recIndex.getIndexes().get(0);
				BigInteger recIndexB = new BigInteger(recIndexBs);
				assertEquals(indexes.get(i), recIndexB);
			}

		}

	}

	private int[] getDestPlayer(int playerSource) {

		int[] players = new int[2];
		switch (playerSource) {
			case 0 :
				players[0] = 1;
				players[1] = 2;
				break;
			case 1 :
				players[0] = 0;
				players[1] = 2;
				break;
			case 2 :
				players[0] = 0;
				players[1] = 1;
		}
		return players;
	}

	private class RSImpl extends TestRegionServer {

		public RSImpl(int playerID) throws IOException {
			super(playerID);
		}

		@Override
		public void doComputation() {
			for (int i = 0; i < indexes.size(); i++) {
				byte[] reqID = ("" + i).getBytes();
				byte[] regionID = "1".getBytes();
				RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
				relay.registerRequest(ident);
				SharemindPlayer player = new ContextPlayer(relay, ident,
						playerID, broker);

				int playerSource = playerSourceIDs.get(i);
				BigInteger index = indexes.get(i);

                throw new IllegalStateException("Test not adjusted to changes");
                /*if (playerID == playerSource) {
                    List<byte[]> indexesToSendBa = new ArrayList<byte[]>();
					indexesToSendBa.add(index.toByteArray());
					FilteredIndexes indexToSend = new FilteredIndexes(
							indexesToSendBa);
					player.sendFilteredIndexes(indexToSend);
				} else {
					FilteredIndexes received = player.getFilterIndexes();
					recIndexes.get(playerID).add(received);
				}*/
                //player.cleanValues();

			}
		}
	}
}
