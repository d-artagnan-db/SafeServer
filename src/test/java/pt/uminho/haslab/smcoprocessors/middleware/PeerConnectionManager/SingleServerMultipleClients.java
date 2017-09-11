package pt.uminho.haslab.smcoprocessors.middleware.PeerConnectionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.PeersConnectionManager;
import pt.uminho.haslab.smcoprocessors.comunication.PeersConnectionManagerImpl;
import pt.uminho.haslab.smcoprocessors.comunication.RelayServer;
import pt.uminho.haslab.smcoprocessors.helpers.RegionServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class SingleServerMultipleClients {
    private static final Log LOG = LogFactory.getLog(SingleServerMultipleClients.class
            .getName());

    final int NCLIENTS = 3;

    private final List<RegionServer> clients;
    PeersConnectionManager clientPeerConnectionManager;
    private RegionServer server;

    SingleServerMultipleClients() {
        clients = new ArrayList<RegionServer>();
        clientPeerConnectionManager = new PeersConnectionManagerImpl(6262);
    }

    @Test
    public void testProtocol() throws InterruptedException, IOException {

        //Function must initiate the server and wait for its correct initialization.
        server = createServer();
        LOG.debug("Going to create clients");
        for (int i = 0; i < NCLIENTS; i++) {
            RegionServer server = createClient(i);
            clients.add(server);
        }

        long start = System.nanoTime();
        LOG.debug("Starting RegionServer");
        for (RegionServer server : clients) {
            server.startRegionServer();
        }
        Thread.sleep(400);
        LOG.debug("Stopping Client regionServers");
        for (RegionServer server : clients) {
            server.stopRegionServer();
        }
        LOG.debug("Stopping Server");
        server.stopRegionServer();
        long end = System.nanoTime();
        long duration = TimeUnit.SECONDS.convert(end - start,
                TimeUnit.NANOSECONDS);
        System.out.println("Execution time was " + duration + " second");
        validateResults();

        // wait some time before next tests
        Thread.sleep(5000);

    }

    protected abstract void validateResults();

    protected abstract RegionServer createClient(int i);


    //Function must initiate the server and wait for its correct initialization.
    protected abstract RegionServer createServer() throws IOException, InterruptedException;


    protected abstract class AbsPlayerServer extends Thread implements RegionServer {

        protected final String bindingAddress;
        protected final int bindingPort;
        protected final MessageBroker broker;
        protected final RelayServer server;
        protected boolean runStatus;

        AbsPlayerServer(String bindingAddress, int bindingPort, MessageBroker broker) throws IOException {
            this.bindingAddress = bindingAddress;
            this.bindingPort = bindingPort;
            this.broker = broker;
            server = new RelayServer(bindingAddress, bindingPort, broker);
            runStatus = false;
        }

        public void startRegionServer() {
            this.start();

        }

        public void stopRegionServer() throws IOException, InterruptedException {
            LOG.debug("Stoping RegionServer");
            server.shutdown();
            this.join();

        }

        public boolean getRunStatus() {
            return runStatus;
        }

    }

    protected abstract class AbsPlayerClient extends Thread implements RegionServer {

        protected final int playerID;
        protected final String ip;
        protected final int port;
        protected boolean runStatus;


        AbsPlayerClient(int playerID, String ip, int port) {
            this.playerID = playerID;
            this.ip = ip;
            this.port = port;
            runStatus = false;
        }

        public void startRegionServer() {
            this.start();

        }

        public void stopRegionServer() throws IOException, InterruptedException {
            this.join();

        }

        public boolean getRunStatus() {
            return runStatus;
        }
    }
}
