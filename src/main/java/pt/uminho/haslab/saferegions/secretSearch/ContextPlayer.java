package pt.uminho.haslab.saferegions.secretSearch;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.saferegions.comunication.*;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * A PlayerRequest is what SharemindValue interacts with, to him it is the
 * current player. But on the HBase context it also contains the information of
 * a request and knows how to handle the the commands from the ShareValue. It
 * knows how to communicate with the relay to send the values to the correct
 * place. For the Message Broker it is also a player that receives messages.
 * <p>
 * The conversion from BigInteger to ShareMessage to byte[] is done in this
 * class. The conversion form byte[] to BigInteger is done on the message
 * broker.
 * <p>
 * Each player has two queues where the values from the other two players are
 * inserted.
 * <p>
 * Some calculations have to be made to determined the correct player based on
 * the current playerId.
 */
public class ContextPlayer implements Player, SharemindPlayer {
	static final Log LOG = LogFactory.getLog(ContextPlayer.class.getName());

	/*
	 * Locks are not used. Just the conditions. Only one thread is writing
	 * (MessageBroker) and only one is reading, the player.
	 */

	private final Relay relay;
	private final RequestIdentifier requestID;
	private final int playerID;

	private final MessageBroker broker;
	/**
	 * When reading messages from the messageBroker, if the messages is not from
	 * the expected player, then store it in in the queue already sorted. Before
	 * reading any messages from the broker check the queue to see if it already
	 * exists.
	 */
	private final Map<Integer, Queue<List<byte[]>>> playerBatchMessages;

	private final Map<Integer, Queue<int[]>> playerBatchMessagesInt;

	private final Map<Integer, Queue<long[]>> playerBatchMessagesLong;

	private final Search.BatchShareMessage.Builder bmBuilder;




	private int targetPlayerID;
	private boolean isTargetPlayer;

	public ContextPlayer(Relay relay, RequestIdentifier requestID,
			int playerID, MessageBroker broker) {

		this.relay = relay;
		this.requestID = requestID;
		this.playerID = playerID;
		this.broker = broker;

		playerBatchMessages = new HashMap<Integer, Queue<List<byte[]>>>();
		playerBatchMessagesInt = new HashMap<Integer, Queue<int[]>>();
		playerBatchMessagesLong = new HashMap<Integer, Queue<long[]>>();

		int[] players = getPlayerSources();

		playerBatchMessages.put(players[0], new LinkedList<List<byte[]>>());
		playerBatchMessages.put(players[1], new LinkedList<List<byte[]>>());


		playerBatchMessagesInt.put(players[0], new LinkedList<int[]>());
		playerBatchMessagesInt.put(players[1], new LinkedList<int[]>());

		playerBatchMessagesLong.put(players[0], new LinkedList<long[]>());
		playerBatchMessagesLong.put(players[1], new LinkedList<long[]>());

		bmBuilder = BatchShareMessage.newBuilder();



	}

	public void sendValueToPlayer(Integer destPlayer, List<byte[]> values) {
		try {

			BatchShareMessage.Builder bsm = bmBuilder
					.setPlayerSource(this.playerID)
					.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
					.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
					.setPlayerDest(destPlayer);

			List<ByteString> bsl = new ArrayList<ByteString>();
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

    public void sendProtocolResults(List<byte[]> res) {
        try {
			List<ByteString> bsValues = new ArrayList<ByteString>();

            for (byte[] val : res) {
                bsValues.add(ByteString.copyFrom(val));
			}

			ResultsMessage msg = ResultsMessage
					.newBuilder()
					.setPlayerSource(this.playerID)
					.setRequestID(ByteString.copyFrom(requestID.getRequestID()))
					.setRegionID(ByteString.copyFrom(requestID.getRegionID()))
					.setPlayerDest(targetPlayerID).addAllValues(bsValues)
                    .build();
            relay.sendProtocolResults(msg);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}

    @Override
    public void sendIntProtocolResults(int[] dest) {
        try {
            CIntBatchShareMessage msg = new CIntBatchShareMessage(this.playerID, targetPlayerID, requestID, dest);
            relay.sendProtocolResults(msg);
        } catch (IOException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }

	@Override
    public void sendLongProtocolResults(long[] results) {

		try {
            CLongBatchShareMessage msg = new CLongBatchShareMessage(this.playerID, targetPlayerID, requestID, results);
            relay.sendProtocolResults(msg);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

	private void sendFilteredIndexesToPlayer(int destPlayer,
                                             int[] toSend) throws IOException {
        CIntBatchShareMessage msg = new CIntBatchShareMessage(this.playerID, destPlayer, requestID, toSend);
        relay.sendFilteredIndexes(msg);

	}

    public void sendFilteredIndexes(int[] indexes) {
        try {

			int[] otherPlayers = getPlayerSources();

			for (int destPlayer : otherPlayers) {
                sendFilteredIndexesToPlayer(destPlayer, indexes);
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
	public List<byte[]> getValues(Integer originPlayerId) {
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
				playerBatchMessages.get(shareMessage.getPlayerSource()).add(
						recMessages);

				return this.getValues(originPlayerId);
			} else {
				return recMessages;
			}


		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

    @Override
    public int[] getIntValues(Integer originPlayerId) {


        if (!playerBatchMessagesInt.get(originPlayerId).isEmpty()) {
            return playerBatchMessagesInt.get(originPlayerId).poll();
        }

        try {
            Queue<CIntBatchShareMessage> messages = broker
                    .getReceivedBatchMessagesInt(requestID);

            while (messages.peek() == null) {

                broker.waitNewBatchMessage(requestID);
            }

            CIntBatchShareMessage shareMessage = messages.poll();
            broker.readBatchMessages(requestID);


            if (shareMessage.getSourcePlayer() != originPlayerId) {
                playerBatchMessagesInt.get(shareMessage.getSourcePlayer()).add(
                        shareMessage.getValues());

                return this.getIntValues(originPlayerId);
            } else {
                return shareMessage.getValues();
            }

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

	@Override
	public long[] getLongValues(Integer originPlayerId) {
		if (!playerBatchMessagesLong.get(originPlayerId).isEmpty()) {
			return playerBatchMessagesLong.get(originPlayerId).poll();
		}

		try {
			Queue<CLongBatchShareMessage> messages = broker
					.getReceivedBatchMessagesLong(requestID);

			while (messages.peek() == null) {

				broker.waitNewBatchMessage(requestID);
			}

			CLongBatchShareMessage shareMessage = messages.poll();
			broker.readBatchMessages(requestID);


			if (shareMessage.getSourcePlayer() != originPlayerId) {
				playerBatchMessagesLong.get(shareMessage.getSourcePlayer()).add(
						shareMessage.getValues());

				return this.getLongValues(originPlayerId);
			} else {
				return shareMessage.getValues();
			}

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}
	}


	@Override
	public void sendValueToPlayer(Integer destPlayer, int[] ints) {
        try {
            CIntBatchShareMessage msg = new CIntBatchShareMessage(this.playerID, destPlayer, requestID, ints);
            relay.sendBatchMessages(msg);
        } catch (IOException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
	}

	@Override
	public void sendValueToPlayer(Integer destPlayer, long[] longs) {
		try {
			CLongBatchShareMessage msg = new CLongBatchShareMessage(this.playerID, destPlayer, requestID, longs);
			relay.sendBatchMessages(msg);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}


	/**
	 * The output should be SearchResults and not DataIdentifiers. That way the
	 * result would be consistent with the output of secretSearch. This function
	 * returns the SeachResults receives the results computed from the other
	 * parties, thus the result returned by this function should be a list with
	 * size equal to two.
	 */
    public List<List<byte[]>> getProtocolResults()
            throws ResultsLengthMismatch {
		Queue<ResultsMessage> messages = broker.getProtocolResults(requestID);
        List<List<byte[]>> results = new ArrayList<List<byte[]>>();

		for (ResultsMessage msg : messages) {
			List<byte[]> values = new ArrayList<byte[]>();

			for (ByteString val : msg.getValuesList()) {
				values.add(val.toByteArray());
			}
            results.add(values);
        }
		messages.clear();
		broker.protocolResultsRead(requestID);

		assert results.size() == 2;
		return results;
	}

	@Override
    public List<int[]> getIntProtocolResults() throws ResultsLengthMismatch {
        Queue<CIntBatchShareMessage> messages = broker.getIntProtocolResults(requestID);
        List<int[]> results = new ArrayList<int[]>();

        for (CIntBatchShareMessage msg : messages) {
            results.add(msg.getValues());
        }
		messages.clear();
		broker.intProtocolResultsRead(requestID);

		assert results.size() == 2;
		return results;
	}

    public List<long[]> getLongProtocolResults() throws ResultsLengthMismatch {
        Queue<CLongBatchShareMessage> messages = broker.getLongProtocolResults(requestID);
        List<long[]> results = new ArrayList<long[]>();

        for (CLongBatchShareMessage msg : messages) {
            results.add(msg.getValues());
        }
		messages.clear();
        broker.longProtocolResultsRead(requestID);

		assert results.size() == 2;
		return results;
	}

    public int[] getFilterIndexes() {
        CIntBatchShareMessage recMessage = broker.getFilterIndexes(requestID);
        broker.indexMessageRead(requestID);
        return recMessage.getValues();

	}

	public void cleanValues() {

		broker.allBatchMessagesRead(requestID);
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
				+ "testing and not implemented on ContextPlayer class");
	}

	public boolean isTargetPlayer() {
		return this.isTargetPlayer;
	}

	public void setTargetPlayer(int targetPlayerID) {
		this.targetPlayerID = targetPlayerID;
		this.isTargetPlayer = this.playerID == targetPlayerID;
	}

	public void cleanResultsMatch() {
		broker.allResultsRead(requestID);
		broker.allBatchMessagesRead(requestID);
		broker.allIndexesMessagesRead(requestID);
	}

	public void storeValues(Integer playerDest, Integer playerSource,
			List<byte[]> values) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public void storeValues(Integer integer, Integer integer1, int[] ints) {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void storeValues(Integer integer, Integer integer1, long[] longs) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void sendValueToPlayer(int playerId, BigInteger value) {
		String msg = "Single ShareMessage are deprecated. "
				+ "       Please send protocols of messages.";
		LOG.error(msg);
		throw new UnsupportedOperationException(msg);
	}

	public BigInteger getValue(Integer originPlayerId) {
		String msg = "Single ShareMessage are deprecated. "
				+ "       Please send protocols of messages.";
		LOG.error(msg);
		throw new UnsupportedOperationException(msg);
	}

}
