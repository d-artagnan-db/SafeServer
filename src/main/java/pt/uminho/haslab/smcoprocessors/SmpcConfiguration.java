package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.smcoprocessors.CMiddleware.IORelay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;

import java.io.IOException;

public class SmpcConfiguration {

    private final int playerID;
    private final String relayHost;
    private final int relayPort;
    private final String relayFirstTargetAddress;
    private final int relayFirstTargetPort;
    private final String relaySecondTargetAddress;
    private final int relaySecondTargetPort;
    private final int waitTime;
    private final int nBits;
    private final boolean isDevelopment;
    private final String secretFamily;
    private final String secretQualifier;
    private final int batchSize;
    private final int preRandomElems;

    public SmpcConfiguration(Configuration conf) {
        playerID = conf.getInt("smhbase.player.id", -1);
        relayHost = conf.get("smhbase.relay.host");
        relayPort = conf.getInt("smhbase.relay.port", -1);
        relayFirstTargetAddress = conf
                .get("smhbase.relay.target.first.address");
        relayFirstTargetPort = conf.getInt("smhbase.relay.target.first.port",
                -1);
        relaySecondTargetAddress = conf
                .get("smhbase.relay.target.second.address");
        relaySecondTargetPort = conf.getInt("smhbase.relay.target.second.port",
                -1);

        waitTime = conf.getInt("smhbase.relay.wait.time", 0);
        nBits = conf.getInt("smhbase.nbits", -1);
        isDevelopment = conf.getBoolean("hbase.coprocessor.development", true);
        secretFamily = conf.get("smhbase.column.family");
        secretQualifier = conf.get("smhbase.column.qualifier");
        batchSize = conf.getInt("smhbase.batch.size", 20);
        preRandomElems = conf.getInt("smhbase.smpc.prerandom.size", 0);

    }

    public Relay createRelay(MessageBroker broker) throws IOException {
        return new IORelay(relayHost, relayPort, relayFirstTargetAddress,
                relayFirstTargetPort, relaySecondTargetAddress,
                relaySecondTargetPort, broker);
    }

    public int getPlayerID() {
        return playerID;
    }

    public String getRelayHost() {
        return relayHost;
    }

    public int getRelayPort() {
        return relayPort;
    }

    public int getWaitTime() {
        return waitTime;
    }

    public int getnBits() {
        return nBits;
    }

    public boolean isIsDevelopment() {
        return isDevelopment;
    }

    public String getPlayerIDasString() {
        return String.valueOf(playerID);
    }

    public byte[] getSecretFamily() {
        return this.secretFamily.getBytes();
    }

    public byte[] getSecretQualifier() {
        return this.secretQualifier.getBytes();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getPreRandomSize() {
        return preRandomElems;
    }

}
