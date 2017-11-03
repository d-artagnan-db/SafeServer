package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.Relay;
import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.smcoprocessors.secretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.secretSearch.SharemindPlayer;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchTestPlayer implements SharemindPlayer {

	private static final Log LOG = LogFactory.getLog(BatchTestPlayer.class
			.getName());
	protected final ContextPlayer player;
	private final Map<Integer, List<List<byte[]>>> messagesSent;
	private final Map<Integer, List<List<byte[]>>> messagesReceived;
	protected RequestIdentifier requestID;

	public BatchTestPlayer(Relay relay, RequestIdentifier requestID,
			int playerID, MessageBroker broker) {
		this.player = new ContextPlayer(relay, requestID, playerID, broker);
		messagesSent = new HashMap<Integer, List<List<byte[]>>>();
		messagesReceived = new HashMap<Integer, List<List<byte[]>>>();
		this.requestID = requestID;

	}

	public void sendValueToPlayer(int playerId, BigInteger value) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void storeValue(Integer playerDest, Integer playerSource,
			BigInteger value) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		player.storeValues(playerDest, playerSource, values);
	}

	public BigInteger getValue(Integer originPlayerId) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public int getPlayerID() {
		return player.getPlayerID();
	}

	public void sendValueToPlayer(Integer playerID, List<byte[]> values) {
		player.sendValueToPlayer(playerID, values);
	}

	public List<byte[]> getValues(Integer rec) {
		return player.getValues(rec);
	}

	public Map<Integer, List<List<byte[]>>> getMessagesSent() {
		return messagesSent;
	}

	public Map<Integer, List<List<byte[]>>> getMessagesReceived() {
		return messagesReceived;
	}

	public void sendProtocolResults(SearchResults res) {
		player.sendProtocolResults(res);
	}

	public List<SearchResults> getProtocolResults()
			throws ResultsLengthMismatch {
		return player.getProtocolResults();
	}

	public void cleanValues() {
		player.cleanValues();
	}

	public void cleanResultsMatch() {
		player.cleanResultsMatch();
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

	public void setTargetPlayer(int targetPlayer) {
		player.setTargetPlayer(targetPlayer);
	}
}
