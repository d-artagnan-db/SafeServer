package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.smcoprocessors.comunication.IORelay;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.Relay;
import pt.uminho.haslab.smcoprocessors.discovery.DiscoveryServiceConfiguration;

import java.io.IOException;

public class SmpcConfiguration {

    //IORelay configuration
    private final int playerID;
    private final String relayHost;
    private final int relayPort;
    private final boolean isDevelopment;

    //SMPC library configuration
    private final int nBits;
    private final int batchSize;
    private final int preRandomElems;

    //DiscoveryService configuration
    private final int sleepTime;
    private final int incTime;
    private final int retries;
    private final String discoveryServiceLocation;

    public SmpcConfiguration(Configuration conf) {

        //IORelay configuration
        playerID = conf.getInt("smhbase.player.id", -1);
        relayHost = conf.get("smhbase.relay.host");
        relayPort = conf.getInt("smhbase.relay.port", -1);
        isDevelopment = conf.getBoolean("hbase.coprocessor.development", true);

        //SMCP library configuration
        nBits = conf.getInt("smhbase.nbits", -1);
        batchSize = conf.getInt("smhbase.protocols.size", 20);
        preRandomElems = conf.getInt("smhbase.smpc.prerandom.size", 0);

        //DiscoveryService configuration
        discoveryServiceLocation = conf.get("smhbase.discovery.location", "localhost");
        sleepTime = conf.getInt("smhbase.discovery.sleepTime", 200);
        incTime = conf.getInt("smhbase.discovery.incTime", 100);
        retries = conf.getInt("smhbase.discovery.retries", 5);
    }

    public Relay createRelay(MessageBroker broker) throws IOException {
        DiscoveryServiceConfiguration conf = new DiscoveryServiceConfiguration(discoveryServiceLocation, playerID, relayHost, relayPort, sleepTime, incTime, retries);
        return new IORelay(relayHost, relayPort, broker, conf);
    }

    public int getPlayerID() {
        return playerID;
    }

    public int getnBits() {
        return nBits;
    }

    public String getPlayerIDasString() {
        return String.valueOf(playerID);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getPreRandomSize() {
        return preRandomElems;
    }

    public String getRelayHost() {
        return relayHost;
    }

    public int getRelayPort() {
        return relayPort;
    }

    public boolean isDevelopment() {
        return isDevelopment;
    }

}
