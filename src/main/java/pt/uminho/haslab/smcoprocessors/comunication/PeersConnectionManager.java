package pt.uminho.haslab.smcoprocessors.comunication;

import java.io.IOException;

/**
 * Interface used by the IORelay to get a RelayClient that abstracts  player in another cluster
 * The Received Relay client can only be used to send messages to the player. Evert Player must establish a connection
 * with other players to send an response.
 */
public interface PeersConnectionManager {


    RelayClient getRelayClient(String host, int port);

    void shutdownClients() throws IOException, InterruptedException;

}
