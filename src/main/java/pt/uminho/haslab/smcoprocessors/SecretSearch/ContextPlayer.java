package pt.uminho.haslab.smcoprocessors.SecretSearch;

import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smhbase.interfaces.Player;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;

/**
 * A PlayerRequest is what SharemindValue interacts with, to him it is the
 * current player.But on the HBase context it also contains the information of a
 * request and knows how to handle the the commands from the ShareValue. It
 * knows how to communicate with the relay to send the values to the correct
 * place. For the Message Broker it is also a player that receives messages.
 * 
 * The conversion from BigInteger to ShareMessage to byte[] is done in this
 * class. The conversion form byte[] to BigInteger is done on the message
 * broker.
 * 
 * Each player has two queues where the values from the other two players are
 * inserted.
 * 
 * Some calculations have to be made to determined the correct player based on
 * the current playerId.
 * 
 */
public class ContextPlayer implements Player, SharemindPlayer {
	static final Log LOG = LogFactory.getLog(ContextPlayer.class.getName());

	/*
	 * Locks are not used.Just the conditions. Only one thread is writing
	 * (MessageBroker) and only one is reading, the player.
	 */

	private final Relay relay;
	private final RequestIdentifier requestID;
	private final int playerID;

	private final MessageBroker broker;

	private boolean isTargetPlayer;

	/**
	 * When reading messages from the messageBroker, if the messages is not from
	 * the expected player, then store it in in the queue already sorted. Before
	 * reading any messages from the broker check the queue to see if it already
	 * exists.
	 */
	private final Map<Integer, Queue<BigInteger>> playerMessages;

	private final Map<Integer, Queue<List<byte[]>>> playerBatchMessages;

	private final ShareMessage.Builder message;
	private final BatchShareMessage.Builder bmBuilder;

	public ContextPlayer(Relay relay, RequestIdentifier requestID,
			int playerID, MessageBroker broker) {

		this.relay = relay;
		this.requestID = requestID;
		this.playerID = playerID;
		this.broker = broker;
		playerMessages = new HashMap<Integer, Queue<BigInteger>>();
		playerBatchMessages = new HashMap<Integer, Queue<List<byte[]>>>();

		int[] players = getPlayerSources();
		playerMessages.put(players[0], new LinkedList<BigInteger>());
		playerMessages.put(players[1], new LinkedList<BigInteger>());

		playerBatchMessages.put(players[0], new LinkedList<List<byte[]>>());
		playerBatchMessages.put(players[1], new LinkedList<List<byte[]>>());

		message = ShareMessage.newBuilder();
		bmBuilder = BatchShareMessage.newBuilder();

	}

	public void sendValueToPlayer(int destPlayer, BigInteger value) {
		try {
			ByteString bsVal = ByteString.copyFrom(value.toByteArray());

			ShareMessage msg = message
					.setPlayerSource(this.playerID)
					.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
					.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
					.setPlayerDest(destPlayer).addValues(bsVal).build();
			relay.sendMessage(msg);
			message.clear();
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}
	public void sendValueToPlayer(Integer destPlayer, List<byte[]> values) {
		try {
			BatchShareMessage.Builder bsm = bmBuilder
					.setPlayerSource(this.playerID)
					.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
					.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
					.setPlayerDest(destPlayer);

          
          
            List<ByteString>  bsl = new ArrayList<ByteString>();
			for (byte[] val : values) {
				ByteString bsVal = ByteString.copyFrom(val);
                bsl.add(bsVal);
			}
			bsm.addAllValues(bsl);
			relay.sendBatchMessages(bsm.build());
			bmBuilder.clear();

		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}

	public void sendProtocolResults(int destPlayer, SearchResults res) {
		try {
			List<ByteString> bsValues = new ArrayList<ByteString>();
			List<ByteString> bsIds = new ArrayList<ByteString>();

			for (byte[] val : res.getSecrets()) {
				bsValues.add(ByteString.copyFrom(val));
			}

			for (byte[] val : res.getIdentifiers()) {
				bsIds.add(ByteString.copyFrom(val));
			}

			ResultsMessage msg = ResultsMessage
					.newBuilder()
					.setPlayerSource(this.playerID)
					.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
					.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
					.setPlayerDest(destPlayer).addAllValues(bsValues)
					.addAllSecretID(bsIds).build();
			relay.sendProtocolResults(msg);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}

	private void sendFilteredIndexesToPlayer(int destPlayer,
			List<ByteString> ids) throws IOException {
		FilterIndexMessage msg = FilterIndexMessage.newBuilder()
				.setPlayerSource(this.playerID)
				.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
				.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
				.setPlayerDest(destPlayer).addAllIndexes(ids).build();
		relay.sendFilteredIndexes(msg);

	}

	public void sendFilteredIndexes(FilteredIndexes indexes) {
		try {
			List<ByteString> ids = new ArrayList<ByteString>();

			for (byte[] val : indexes.getIndexes()) {
				ids.add(ByteString.copyFrom(val));
			}

			int[] otherPlayers = getPlayerSources();

			for (int destPlayer : otherPlayers) {
				sendFilteredIndexesToPlayer(destPlayer, ids);
			}

		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}

	/**
	 * This method has to be synchronized because the message broker might be
	 * storing a value on the queues. Since it is synchronized it blocks until
	 * the message broker has inserted the value.
	 * 
	 * @param originPlayerId
	 * @return BigInteger of the value sent from originPlayerId.
	 */
	public BigInteger getValue(Integer originPlayerId) {
		// LOG.debug("Going to call getValue");
		/**
		 * First check for messages already stored when reading another value.
		 * Since values are not received in order, the player may read a value
		 * from another player besides the one it is expecting from. When this
		 * happens it stores in playersMessages variable.
		 */
		if (!playerMessages.get(originPlayerId).isEmpty()) {

			return playerMessages.get(originPlayerId).poll();
		}

		try {
			Queue<ShareMessage> messages = broker
					.getReceivedMessages(requestID);

			while (messages.peek() == null) {
				broker.waitNewMessage(requestID);
			}

			ShareMessage shareMessage = messages.poll();
			broker.readMessages(requestID);

			// ATENTION: VALUE HARD CODED TO FIRST ELEMEENT
			// IN CASE OF BATCH MESSAGE, IT WILL HAVE MORE THAN ONE ELEMENT
			byte[] val = shareMessage.getValues(0).toByteArray();
			BigInteger bVal = new BigInteger(val);

			if (shareMessage.getPlayerSource() != originPlayerId) {
				// LOG.debug("Going to call again getValue");
				playerMessages.get(shareMessage.getPlayerSource()).add(bVal);
				BigInteger resultValue = this.getValue(originPlayerId);
				return resultValue;
			} else {
				return bVal;
			}

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		} finally {
		}
	}

	/**
	 * This method has to be synchronized because the message broker might be
	 * storing a value on the queues. Since it is synchronized it blocks until
	 * the message broker has inserted the value.
	 * 
	 * @param originPlayerId
	 * @return BigInteger of the value sent from originPlayerId.
	 */
	public List<byte[]> getValues(Integer originPlayerId) {
		// LOG.debug("Going to call getValue");
		/**
		 * First check for messages already stored when reading another value.
		 * Since values are not received in order, the player may read a value
		 * from another player besides the one it is expecting from. When this
		 * happens it stores in playersMessages variable.
		 */
		if (!playerBatchMessages.get(originPlayerId).isEmpty()) {

			return playerBatchMessages.get(originPlayerId).poll();
		}

		try {
			Queue<BatchShareMessage> messages = broker
					.getReceivedBatchMessages(requestID);

			while (messages.peek() == null) {
				broker.waitNewBatchMessage(requestID);
			}

			BatchShareMessage shareMessage = messages.poll();
			broker.readBatchMessages(requestID);

			List<byte[]> recMessages = new ArrayList<byte[]>();
			List<ByteString> recbMessages = shareMessage.getValuesList();

			for (ByteString bs : recbMessages) {
				recMessages.add(bs.toByteArray());
			}

			if (shareMessage.getPlayerSource() != originPlayerId) {
				// LOG.debug("Going to call again getValue");
				playerBatchMessages.get(shareMessage.getPlayerSource()).add(
						recMessages);
				return this.getValues(originPlayerId);
			} else {
				return recMessages;
			}

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		} finally {
		}
	}

	/**
	 * The output should be SearchResults and not DataIdentifiers. That way the
	 * result would be consistent with the output of SecretSearch.
	 */
	public List<SearchResults> getProtocolResults()
			throws ResultsLengthMissmatch {
		Queue<ResultsMessage> messages = broker.getProtocolResults(requestID);
		List<SearchResults> results = new ArrayList<SearchResults>();

		for (ResultsMessage msg : messages) {
			List<byte[]> identifiers = new ArrayList<byte[]>();
			List<byte[]> values = new ArrayList<byte[]>();

			for (ByteString ident : msg.getSecretIDList()) {
				identifiers.add(ident.toByteArray());
			}

			for (ByteString val : msg.getValuesList()) {
				values.add(val.toByteArray());
			}
			results.add(new SearchResults(values, identifiers));
		}

		broker.protocolResultsRead(requestID);
		return results;
	}

	@Override
	public FilteredIndexes getFilterIndexes() {
		FilterIndexMessage recMessage = broker.getFilterIndexes(requestID);

		List<byte[]> indexes = new ArrayList<byte[]>();

		for (ByteString bs : recMessage.getIndexesList()) {
			indexes.add(bs.toByteArray());
		}

		broker.indexeMessageRead(requestID);
		return new FilteredIndexes(indexes);

	}

	public void cleanValues() {

		broker.allMessagesRead(requestID);
		broker.allResultsRead(requestID);
		broker.allIndexesMessagesRead(requestID);
	}

	// Returns a list of the other players ids.
	private int[] getPlayerSources() {

		int[] players = new int[2];
		switch (playerID) {
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

	public int getPlayerID() {
		return this.playerID;
	}

	public void storeValue(Integer intgr, Integer intgr1, BigInteger bi) {
		throw new UnsupportedOperationException("Operation only used for "
				+ "testing and not implemente on ContextPlayer class");
	}

	public boolean isTargetPlayer() {
		return isTargetPlayer;
	}

	public void setTargetPlayer() {
		isTargetPlayer = true;
	}

	public void cleanResultsMatch() {
		broker.allResultsRead(requestID);
		broker.allMessagesRead(requestID);
		broker.allIndexesMessagesRead(requestID);
	}

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		throw new UnsupportedOperationException("Not supported yet."); // To
																		// change
																		// body
																		// of
																		// generated
																		// methods,
																		// choose
																		// Tools
																		// |
																		// Templates.
	}

}
