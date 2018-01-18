package pt.uminho.haslab.saferegions.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.secretSearch.ContextPlayer;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ConcurrentBatchTestPlayer extends Thread
		implements
        Player {

	private static final Log LOG = LogFactory
			.getLog(ConcurrentBatchTestPlayer.class.getName());

	protected final Map<Integer, List<List<byte[]>>> messagesSent;

	protected final Map<Integer, List<List<byte[]>>> messagesReceived;

	protected final ContextPlayer player;

	protected final List<byte[]> firstValueSecret;

	protected final List<byte[]> secondValueSecret;

	protected final int nBits;

	protected List<byte[]> resultSecret;

	protected RequestIdentifier requestID;

	public ConcurrentBatchTestPlayer(Relay relay, RequestIdentifier requestID,
			int playerID, MessageBroker broker, List<byte[]> firstVals,
			List<byte[]> secondVals, int nBits) {
		this.player = new ContextPlayer(relay, requestID, playerID, broker);
		messagesSent = new HashMap<Integer, List<List<byte[]>>>();
		messagesReceived = new HashMap<Integer, List<List<byte[]>>>();

		firstValueSecret = firstVals;
		secondValueSecret = secondVals;

		this.nBits = nBits;
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

	@Override
	public void run() {
		resultSecret = testingProtocol(firstValueSecret, secondValueSecret);
	}

	public List<byte[]> getResultSecret() {
		return this.resultSecret;
	}

	public void startProtocol() {
		this.start();
	}

	public void waitEndOfProtocol() throws InterruptedException {
		this.join();
	}

	protected abstract List<byte[]> testingProtocol(
			List<byte[]> firstValueSecret, List<byte[]> secondValueSecret);

	@Override
	public void storeValues(Integer integer, Integer integer1, int[] ints) {
		player.storeValues(integer, integer1, ints);
	}

	@Override
	public void sendValueToPlayer(Integer integer, int[] ints) {
		player.sendValueToPlayer(integer, ints);
	}

	@Override
	public int[] getIntValues(Integer integer) {
		return player.getIntValues(integer);
	}

    @Override
    public void storeValues(Integer integer, Integer integer1, long[] ints) {
        player.storeValues(integer, integer1, ints);
    }

    @Override
    public void sendValueToPlayer(Integer integer, long[] longs) {
        player.sendValueToPlayer(integer, longs);
    }

    @Override
    public long[] getLongValues(Integer integer) {
        return player.getLongValues(integer);
    }
}
