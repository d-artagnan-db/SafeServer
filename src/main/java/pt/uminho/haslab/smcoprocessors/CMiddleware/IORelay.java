package pt.uminho.haslab.smcoprocessors.CMiddleware;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.smcoprocessors.middleware.discovery.*;

import java.io.IOException;
import java.util.List;

public class IORelay implements Relay {

    private static final Log LOG = LogFactory.getLog(IORelay.class.getName());

    private final RelayServer server;
    private final MessageBroker broker;
    private final DiscoveryService discoveryService;
    private final PeersConnectionManager peerConnectionManager;
    private boolean running;

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

    private RelayClient getTargetClient(int playerID, RequestIdentifier requestIdentifier) {
        RelayClient client = null;

        try {
            List<RegionLocation> locations = this.discoveryService.discoverRegions(requestIdentifier);

            for (RegionLocation location : locations) {
                if (location.getPlayerID() == playerID) {
                    client = peerConnectionManager.getRelayClient(location.getIp(), location.getPort());
                    break;
                }
            }
        } catch (FailedRegionDiscovery failedRegionDiscovery) {
            LOG.error(failedRegionDiscovery);
            throw new IllegalStateException(failedRegionDiscovery);
        }
        return client;
    }

    public synchronized void sendBatchMessages(BatchShareMessage msg)
            throws IOException {
        RequestIdentifier ident = new RequestIdentifier(msg.getRequestID().toByteArray(), msg.getRegionID().toByteArray());
        getTargetClient(msg.getPlayerDest(), ident).sendBatchMessages(msg);
    }

    public synchronized void sendProtocolResults(ResultsMessage msg)
            throws IOException {
        RequestIdentifier ident = new RequestIdentifier(msg.getRequestID().toByteArray(), msg.getRegionID().toByteArray());
        getTargetClient(msg.getPlayerDest(), ident).sendProtocolResults(msg);
    }

    public synchronized void sendFilteredIndexes(FilterIndexMessage msg)
            throws IOException {
        RequestIdentifier ident = new RequestIdentifier(msg.getRequestID().toByteArray(), msg.getRegionID().toByteArray());
        getTargetClient(msg.getPlayerDest(), ident).sendFilteredIndexes(msg);

    }

    public void registerRequest(RequestIdentifier requestIdentifier) {
        discoveryService.registerRegion(requestIdentifier);
    }

    public void unregisterRequest(RequestIdentifier requestIdentifier) {
        discoveryService.unregisterRegion(requestIdentifier);
    }

}
