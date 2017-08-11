package pt.uminho.haslab.smcoprocessors.CMiddleware;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.smcoprocessors.discovery.DiscoveryService;
import pt.uminho.haslab.smcoprocessors.discovery.DiscoveryServiceConfiguration;
import pt.uminho.haslab.smcoprocessors.discovery.RedisDiscoveryService;

import java.io.IOException;

public class IORelay implements Relay {

    private static final Log LOG = LogFactory.getLog(IORelay.class.getName());

    private final RelayServer server;

    private boolean running;

    private final MessageBroker broker;

    private final DiscoveryService discoveryService;

    private final PeersConnectionManager peerConnectionManager;

    public IORelay(String bindingAddress, int bindingPort,
                   MessageBroker broker, DiscoveryServiceConfiguration conf) throws IOException {

        server = new RelayServer(bindingAddress, bindingPort, broker);
        discoveryService = new RedisDiscoveryService(conf);
        peerConnectionManager = new PeersConnectionManagerImpl(bindingPort);

        this.running = false;
        this.broker = broker;

    }

    public void stopRelay() throws IOException {
        try {

            LOG.info(server.getBindingPort() + " going to stop relay");
            peerConnectionManager.shutdownClients();
            server.shutdown();
            LOG.info(server.getBindingPort() + " relay stopped");

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }

    public void forceStopRelay() throws IOException {
        try {
            LOG.debug(server.getBindingPort() + " going to force stop relay");
            peerConnectionManager.shutdownClients();
            server.forceShutdown();
            LOG.debug(server.getBindingPort() + " relay force stopped");
        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }

    public void stopErrorRelay() throws InterruptedException, IOException {
        peerConnectionManager.shutdownClients();
        server.shutdown();
    }

    public boolean isRelayRunning() {
        return running;
    }

    private void startServer() throws IOException {

        server.startServer();
        this.broker.relayStarted();
        this.running = true;
    }

    public void bootRelay() {

        try {
            int bp = server.getBindingPort();
            this.startServer();
            LOG.info(bp + " completed booting phase");
        } catch (IOException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }

    private RelayClient getTargetClient(int playerID) {
        //this.discoveryService.
        return null;
    }

    public synchronized void sendBatchMessages(Search.BatchShareMessage msg)
            throws IOException {
        int target = calculateDestPlayer(msg.getPlayerSource(),
                msg.getPlayerDest());

        getTargetClient(target).sendBatchMessages(msg);
    }

    public synchronized void sendProtocolResults(ResultsMessage msg)
            throws IOException {
        int target = calculateDestPlayer(msg.getPlayerSource(),
                msg.getPlayerDest());
        getTargetClient(target).sendProtocolResults(msg);
    }

    public synchronized void sendFilteredIndexes(FilterIndexMessage msg)
            throws IOException {
        int target = calculateDestPlayer(msg.getPlayerSource(),
                msg.getPlayerDest());
        getTargetClient(target).sendFilteredIndexes(msg);

    }

    private int calculateDestPlayer(int playerID, int playerDest) {
        /**
         * return 1 is connection two on NioRelay. return 0 is connection one on
         * NioRelay. originPlayerId is the player destination from the three
         * (0,1,2)
         */

        switch (playerID) {
        /*
         * if this player is 0 and wants to send to player one, then use
		 * connection two on the nio relay. Use connection one if it goes to
		 * player 2.
		 */
            case 0:
                return playerDest == 1 ? 1 : 0;
                /*
				 * if this player is 1 and wants to send to player two, then use
				 * connection two on the nio relay. Use connection one if it
				 * goes to player 0.
				 */
            case 1:
                return playerDest == 2 ? 1 : 0;
				/*
				 * if this player is 2 and wants to send to player zero, then
				 * use connection two on the nio relay. Use connection one if it
				 * goes to player 1.
				 */
            case 2:
                return playerDest == 0 ? 1 : 0;
        }

		/* This does nothing, it just helps netbeans not show a warning */
        return -1;
    }
}
