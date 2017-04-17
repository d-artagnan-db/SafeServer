package pt.uminho.haslab.smcoprocessors.benchmarks;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.protocolresults.DataIdentifiers;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;

public class TestPlayer implements SharemindPlayer {
	private static final Log LOG = LogFactory
			.getLog(TestPlayer.class.getName());

	private final Map<Integer, List<BigInteger>> messagesSent;

	private final Map<Integer, List<BigInteger>> messagesReceived;

	private final Map<Integer, List<List<byte[]>>> batchMessagesSent;

	private final Map<Integer, List<List<byte[]>>> batchMessagesReceived;

	private final ContextPlayer player;

	public TestPlayer(Relay relay, RequestIdentifier requestID, int playerID,
			MessageBroker broker) {
		player = new ContextPlayer(relay, requestID, playerID, broker);
		messagesSent = new HashMap<Integer, List<BigInteger>>();
		messagesReceived = new HashMap<Integer, List<BigInteger>>();
		batchMessagesSent = new HashMap<Integer, List<List<byte[]>>>();
		batchMessagesReceived = new HashMap<Integer, List<List<byte[]>>>();
	}

	@Override
	public BigInteger getValue(Integer originPlayerID) {
		// LOG.debug("Going to call super getValue");
		BigInteger res = player.getValue(originPlayerID);

		if (!messagesReceived.containsKey(originPlayerID)) {
			messagesReceived.put(originPlayerID, new ArrayList<BigInteger>());
		}
		messagesReceived.get(originPlayerID).add(res);

		return res;
	}

	@Override
	public void sendValueToPlayer(int destPlayer, BigInteger value) {

		if (!messagesSent.containsKey(destPlayer)) {
			messagesSent.put(destPlayer, new ArrayList<BigInteger>());
		}

		messagesSent.get(destPlayer).add(value);

		player.sendValueToPlayer(destPlayer, value);
	}

	public Map<Integer, List<BigInteger>> getMessagesSent() {
		return messagesSent;
	}

	public Map<Integer, List<BigInteger>> getMessagesReceived() {
		return messagesReceived;
	}

	public void storeValue(Integer playerDest, Integer playerSource,
			BigInteger value) {
		player.storeValue(playerDest, playerDest, value);
	}

	public int getPlayerID() {
		return player.getPlayerID();
	}

	public void sendProtocolResults(int destPlayer, SearchResults res) {
		player.sendProtocolResults(destPlayer, res);
	}

	public List<SearchResults> getProtocolResults()
			throws ResultsLengthMissmatch {
		return player.getProtocolResults();
	}

	public void cleanValues() {
		player.cleanValues();
	}

	public void sendFilteredIndexes(FilteredIndexes indexes) {
		player.sendFilteredIndexes(indexes);
	}

	public FilteredIndexes getFilterIndexes() {
		return player.getFilterIndexes();
	}

	public boolean isTargetPlayer() {
		return player.isTargetPlayer();
	}

	public void setTargetPlayer() {
		player.setTargetPlayer();
	}

	public void cleanResultsMatch() {
		player.cleanResultsMatch();
	}

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		player.storeValues(playerDest, playerDest, values);
	}

	public void sendValueToPlayer(Integer destPlayer, List<byte[]> values) {

		if (!batchMessagesSent.containsKey(destPlayer)) {
			batchMessagesSent.put(destPlayer, new ArrayList<List<byte[]>>());
		}

		batchMessagesSent.get(destPlayer).add(values);

		player.sendValueToPlayer(destPlayer, values);
	}

	public List<byte[]> getValues(Integer originPlayerID) {
		List<byte[]> res = player.getValues(originPlayerID);

		if (!batchMessagesReceived.containsKey(originPlayerID)) {
			batchMessagesReceived.put(originPlayerID,
					new ArrayList<List<byte[]>>());
		}
		batchMessagesReceived.get(originPlayerID).add(res);

		return res;
	}

}
