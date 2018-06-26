package pt.uminho.haslab.saferegions.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.saferegions.secretSearch.ContextPlayer;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;

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

    @Override
    public void storeValues(Integer integer, Integer integer1, int[] ints) {

    }

    @Override
    public void storeValues(Integer integer, Integer integer1, long[] longs) {

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
    public void sendValueToPlayer(Integer integer, int[] ints) {
        player.sendValueToPlayer(integer, ints);
    }

    @Override
    public void sendValueToPlayer(Integer integer, long[] longs) {

    }

    @Override
    public int[] getIntValues(Integer integer) {
        return player.getIntValues(integer);
    }

    @Override
    public long[] getLongValues(Integer integer) {
        return new long[0];
    }

    public Map<Integer, List<List<byte[]>>> getMessagesSent() {
        return messagesSent;
    }

    public Map<Integer, List<List<byte[]>>> getMessagesReceived() {
        return messagesReceived;
    }

    public void sendProtocolResults(List<byte[]> res) {
        player.sendProtocolResults(res);
    }

    @Override
    public void sendIntProtocolResults(int[] dest) {
        player.sendIntProtocolResults(dest);
    }

    @Override
    public void sendLongProtocolResults(long[] dest) {
        player.sendLongProtocolResults(dest);
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

    public void cleanResultsMatch() {
        player.cleanResultsMatch();
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
}
