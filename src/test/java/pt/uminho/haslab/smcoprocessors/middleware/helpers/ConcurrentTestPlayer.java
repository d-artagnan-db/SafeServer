package pt.uminho.haslab.smcoprocessors.middleware.helpers;

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
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Player;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;

/**
 * Warning: The core functionality of this class is duplicated on Test Player.
 * Correct this issue if possible.
 */
public abstract class ConcurrentTestPlayer extends Thread implements Player {

	private static final Log LOG = LogFactory.getLog(ConcurrentTestPlayer.class
			.getName());

	protected final Map<Integer, List<BigInteger>> messagesSent;

	protected final Map<Integer, List<BigInteger>> messagesReceived;

	protected final ContextPlayer player;

	protected final BigInteger firstValueSecret;

	protected final BigInteger secondValueSecret;

	protected final int nBits;

	protected SharemindSecret resultSecret;

	protected RequestIdentifier requestID;

	public ConcurrentTestPlayer(Relay relay, RequestIdentifier requestID,
			int playerID, MessageBroker broker, BigInteger firstValueSecret,
			BigInteger secondValueSecret, int nBits) {
		player = new ContextPlayer(relay, requestID, playerID, broker);
		messagesSent = new HashMap<Integer, List<BigInteger>>();
		messagesReceived = new HashMap<Integer, List<BigInteger>>();
		this.firstValueSecret = firstValueSecret;
		this.secondValueSecret = secondValueSecret;
		this.nBits = nBits;
		this.requestID = requestID;

	}

	@Override
	public BigInteger getValue(Integer originPlayerID) {
		LOG.debug("Going to call super getValue");
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

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		player.storeValues(playerDest, playerSource, values);
	}

	public void sendValueToPlayer(Integer playerID, List<byte[]> values) {
		player.sendValueToPlayer(playerID, values);
	}

	public List<byte[]> getValues(Integer rec) {
		return player.getValues(rec);
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

	private BigInteger getMod(int nbits) {
		return BigInteger.valueOf(2).pow(nbits + 1);
	}

	private SharemindSecret generateSecret(int nbits, BigInteger value,
			Player player) throws InvalidSecretValue {
		return new SharemindSecret(nbits + 1, getMod(nbits), value, player);
	}

	public int getPlayerID() {
		return player.getPlayerID();
	}

	protected abstract SharemindSecret testingProtocol(Secret originalSecret,
			Secret cmpSecret);

	@Override
	public void run() {
		try {
			Secret fSecret = generateSecret(nBits, firstValueSecret, this);
			Secret sSecret = generateSecret(nBits, secondValueSecret, this);
			resultSecret = testingProtocol(fSecret, sSecret);

		} catch (InvalidSecretValue ex) {
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		}

	}

	public SharemindSecret getResultSecret() {
		return this.resultSecret;
	}

	public void startProtocol() {
		this.start();
	}

	public void waitEndOfProtocol() throws InterruptedException {
		this.join();
	}
}
