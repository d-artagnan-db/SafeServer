package pt.uminho.haslab.smcoprocessors.middleware.relay;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.protocolresults.DataIdentifiers;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.testingutils.ValuesGenerator;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ProtocolResultsTest {

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.protocolResultsGenerator();
	}

	/**
	 * This list contains a sequence of player IDs, that dictates the target of
	 * player that must receive the results of protocol.
	 * 
	 * Each value in the list has one of the following possible values (0,1,2)
	 * that is a playerID. The size of the list dictates how many concurrent
	 * players are created by each RegionServer. The index of the list is used
	 * as the requestID used to exchange messages.
	 */
	private final List<Integer> playerDestIDs;

	// Number of messages to exchange between players;
	private final int nMessages;

	// size of BigInteger Numbers
	private final int nBits;

	public ProtocolResultsTest(List<Integer> playerDestIDs, int nMessages,
			int nBits) {
		this.playerDestIDs = playerDestIDs;
		this.nBits = nBits;
		this.nMessages = nMessages;
	}

	private class RegionServerImpl extends TestRegionServer {

		private final List<List<BigInteger>> values;
		private final List<List<SearchResults>> peerResults;

		public RegionServerImpl(int playerID) throws IOException {
			super(playerID);
			values = new ArrayList<List<BigInteger>>();
			peerResults = new ArrayList<List<SearchResults>>();
		}

		public List<List<SearchResults>> getPeerResults() {
			return peerResults;
		}

		public List<List<BigInteger>> getValues() {
			return values;
		}

		@Override
		public void doComputation() {

			List<Thread> players = new ArrayList<Thread>();

			for (int i = 0; i < playerDestIDs.size(); i++) {
				ConcurrentPlayer p = new ConcurrentPlayer(playerID, i, relay,
						broker);
				players.add(p);
			}

			for (Thread t : players) {
				t.start();
			}

			for (Thread t : players) {
				try {
					t.join();
				} catch (InterruptedException ex) {
					throw new IllegalStateException(ex);
				}
			}

			for (Thread p : players) {
				ConcurrentPlayer player = (ConcurrentPlayer) p;
				/**
				 * Check which is the target player for this requests, and add
				 * the values to the correct list. Add null to the other list,
				 * so that they have the same number of values and when
				 * iterating the lists, the positions match. if there is a null
				 * on a list it means the value is on the other list.
				 */

				if (playerID == playerDestIDs.get(player.getResquestID())) {
					values.add(null);
					peerResults.add(player.getPeerResults());
				} else {
					peerResults.add(null);
					values.add(player.getValues());
				}

			}

		}

	}

	private class ConcurrentPlayer extends Thread {
		private final Random random;
		private final int playerID;
		private final int requestID;
		private final MessageBroker broker;
		private final Relay relay;
		private List<BigInteger> values;
		private List<SearchResults> peerResults;

		public ConcurrentPlayer(int playerID, int requestID, Relay relay,
				MessageBroker broker) {
			random = new Random();
			this.playerID = playerID;
			this.requestID = requestID;
			this.relay = relay;
			this.broker = broker;
		}

		public int getResquestID() {
			return this.requestID;
		}

		public List<BigInteger> getValues() {
			return this.values;
		}

		public List<SearchResults> getPeerResults() {
			return this.peerResults;
		}

		@Override
		public void run() {
			try {
				byte[] reqID = ("" + requestID).getBytes();
				byte[] regionID = "1".getBytes();
				RequestIdentifier ident = new RequestIdentifier(reqID, regionID);

				SharemindPlayer player = new ContextPlayer(relay, ident,
						playerID, broker);

				values = new ArrayList<BigInteger>();
				List<byte[]> valuesToSend = new ArrayList<byte[]>();

				List<byte[]> ids = new ArrayList<byte[]>();
				for (int i = 0; i < nMessages; i++) {
					BigInteger val = new BigInteger(nBits, random);
					BigInteger id = BigInteger.valueOf(i);
					values.add(val);
					valuesToSend.add(val.toByteArray());
					ids.add(id.toByteArray());
				}

				int target = playerDestIDs.get(requestID);
				if (playerID != target) {

					player.sendProtocolResults(target, new SearchResults(
							valuesToSend, ids));
				} else {
					peerResults = player.getProtocolResults();
					player.cleanValues();
				}

			} catch (ResultsLengthMissmatch ex) {
				throw new IllegalStateException(ex);
			}

		}

	}

	@Test
	public void sendProtocolResults() throws IOException, InterruptedException {
		RegionServerImpl a = new RegionServerImpl(0);
		RegionServerImpl b = new RegionServerImpl(1);
		RegionServerImpl c = new RegionServerImpl(2);

		a.startRegionServer();
		b.startRegionServer();
		c.startRegionServer();

		a.stopRegionServer();
		b.stopRegionServer();
		c.stopRegionServer();

		/**
		 * Iterate over each player dest and check if the values were sent
		 * correctly.
		 */
		for (int i = 0; i < playerDestIDs.size(); i++) {

			List<BigInteger> peerOneValues = new ArrayList<BigInteger>();
			List<BigInteger> peerTwoValues = new ArrayList<BigInteger>();
			List<SearchResults> receivedPeerResults = new ArrayList<SearchResults>();

			switch (playerDestIDs.get(i)) {
				case 0 :
					receivedPeerResults = a.getPeerResults().get(i);
					peerOneValues = b.getValues().get(i);
					peerTwoValues = c.getValues().get(i);
					break;
				case 1 :
					receivedPeerResults = b.getPeerResults().get(i);
					peerOneValues = a.getValues().get(i);
					peerTwoValues = c.getValues().get(i);
					break;
				case 2 :
					receivedPeerResults = c.getPeerResults().get(i);
					peerOneValues = a.getValues().get(i);
					peerTwoValues = b.getValues().get(i);
					break;
			}

			assertEquals(peerOneValues.size(), peerTwoValues.size());
			assertEquals(peerOneValues.size(), receivedPeerResults.get(0)
					.getSecrets().size());
			assertEquals(peerOneValues.size(), receivedPeerResults.get(1)
					.getSecrets().size());
			assertEquals(peerOneValues.size(), receivedPeerResults.get(0)
					.getIdentifiers().size());
			assertEquals(peerOneValues.size(), receivedPeerResults.get(1)
					.getIdentifiers().size());

			/**
			 * Check if the results sent to the Dest player all arrive and in
			 * the correct order. Assuming the values are in the same order,
			 * than the first value from one of the list must match.
			 */
			int firstList = 0;
			int secondList = 1;

			if (!peerOneValues.get(0).equals(
					receivedPeerResults.get(0).getSecrets().get(0))) {
				firstList = 1;
				secondList = 0;
			}

			for (int j = 0; j < peerOneValues.size(); j++) {
				assertEquals(peerOneValues.get(j),
						receivedPeerResults.get(firstList).getSecrets().get(j));
				assertEquals(peerTwoValues.get(j),
						receivedPeerResults.get(secondList).getSecrets().get(j));
			}

		}
	}

}
