package pt.uminho.haslab.saferegions.benchmarks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.saferegions.secretSearch.ContextPlayer;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	public BigInteger getValue(Integer originPlayerID) {
		BigInteger res = player.getValue(originPlayerID);

		if (!messagesReceived.containsKey(originPlayerID)) {
			messagesReceived.put(originPlayerID, new ArrayList<BigInteger>());
		}
		messagesReceived.get(originPlayerID).add(res);

		return res;
	}

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

	public void sendProtocolResults(List<byte[]> res) {
		player.sendProtocolResults(res);
	}

	@Override
	public void sendIntProtocolResults(int[] dest) {
		this.player.sendIntProtocolResults(dest);
	}

    @Override
    public void sendLongProtocolResults(long[] dest) {
        this.player.sendLongProtocolResults(dest);
    }

    public List<List<byte[]>> getProtocolResults()
			throws ResultsLengthMismatch {
		return player.getProtocolResults();
	}

	@Override
    public List<int[]> getIntProtocolResults() throws ResultsLengthMismatch {
        return player.getIntProtocolResults();
	}

    @Override
    public List<long[]> getLongProtocolResults() throws ResultsLengthMismatch {
        return player.getLongProtocolResults();
    }

    public void cleanValues() {
		player.cleanValues();
	}

    public void sendFilteredIndexes(int[] indexes) {
        player.sendFilteredIndexes(indexes);
	}

    public int[] getFilterIndexes() {
        return player.getFilterIndexes();
	}

	public boolean isTargetPlayer() {
		return player.isTargetPlayer();
	}

	public void setTargetPlayer(int targetPlayer) {
		player.setTargetPlayer(targetPlayer);
	}

	public void cleanResultsMatch() {
		player.cleanResultsMatch();
	}

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		player.storeValues(playerDest, playerDest, values);
	}

	@Override
	public void storeValues(Integer integer, Integer integer1, int[] ints) {
		player.storeValues(integer, integer1, ints);
	}

    @Override
    public void storeValues(Integer integer, Integer integer1, long[] longs) {
        player.storeValues(integer, integer1, longs);
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

	@Override
	public void sendValueToPlayer(Integer integer, int[] ints) {
		player.sendValueToPlayer(integer, ints);
	}

    @Override
    public void sendValueToPlayer(Integer integer, long[] longs) {
        player.sendValueToPlayer(integer, longs);
    }

    @Override
	public int[] getIntValues(Integer integer) {
		return player.getIntValues(integer);
	}

    @Override
    public long[] getLongValues(Integer integer) {
        return player.getLongValues(integer);
    }

}
