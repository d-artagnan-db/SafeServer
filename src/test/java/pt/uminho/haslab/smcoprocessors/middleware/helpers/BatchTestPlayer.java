package pt.uminho.haslab.smcoprocessors.middleware.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchTestPlayer implements Player {

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
}
