package pt.uminho.haslab.smcoprocessors.middleware.PeerConnectionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.RelayServer;
import pt.uminho.haslab.smcoprocessors.helpers.RegionServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class MultipleServersMultipleClients {
    final static int NCLIENTS = 20;
    final static int NSERVERS = 5;
    final static int NMESSAGES = 10;
    private static final Log LOG = LogFactory.getLog(MultipleServersMultipleClients.class
            .getName());
    final List<String> clientConnectionTargetAddress;
    final List<Integer> clientConnectionTargetPort;
    final CountDownLatch allServersStarted;
    final CountDownLatch allClientsStarted;
    final CountDownLatch allMessagesReceived;
    private final List<RegionServer> servers;
    private final List<RegionServer> clients;
    private final List<String> serverBindingAddressess;
    private final List<Integer> serverBindingPorts;

    MultipleServersMultipleClients(List<String> serverBindingAddressess,
                                   List<Integer> serverBindingPorts,
                                   List<String> clientConnectionTargetAddress,
                                   List<Integer> clientConnectionTargetPort) {
        this.serverBindingAddressess = serverBindingAddressess;
        this.serverBindingPorts = serverBindingPorts;
        this.clientConnectionTargetAddress = clientConnectionTargetAddress;
        this.clientConnectionTargetPort = clientConnectionTargetPort;
        this.servers = new ArrayList<RegionServer>();
        this.clients = new ArrayList<RegionServer>();
        allServersStarted = new CountDownLatch(NSERVERS);
        allClientsStarted = new CountDownLatch(NCLIENTS);
        allMessagesReceived = new CountDownLatch(NCLIENTS * NMESSAGES);

    }


    @Test
    public void testProtocol() throws InterruptedException, IOException {

        //Function must initiate the server and wait for its correct initialization.
        for (int i = 0; i < NSERVERS; i++) {
            servers.add(createServer(serverBindingAddressess.get(i), serverBindingPorts.get(i)));
        }
        LOG.debug("Going to create clients");
        for (int i = 0; i < NCLIENTS; i++) {
            RegionServer client = createClient(clientConnectionTargetAddress.get(i), clientConnectionTargetPort.get(i));
            clients.add(client);
        }

        long start = System.nanoTime();
        LOG.debug("Start Servers");
        for (RegionServer server : servers) {
            server.startRegionServer();
        }
        Thread.sleep(400);
        allServersStarted.await();

        LOG.debug("Start Clients");
        for (RegionServer client : clients) {
            client.startRegionServer();
        }
        LOG.debug("Await for clients to start");
        allClientsStarted.await();
        LOG.debug("Await for messages to be received");
        allMessagesReceived.await();
        LOG.debug("Stop Clients");
        for (RegionServer client : clients) {
            client.stopRegionServer();
        }
        LOG.debug("Stop Servers");
        for (RegionServer server : servers) {
            server.stopRegionServer();
        }
        long end = System.nanoTime();
        long duration = TimeUnit.SECONDS.convert(end - start,
                TimeUnit.NANOSECONDS);
        System.out.println("Execution time was " + duration + " second");
        validateResults();

        // wait some time before next tests
        Thread.sleep(5000);

    }

    protected abstract RegionServer createServer(String s, Integer integer) throws IOException, InterruptedException;

    protected abstract RegionServer createClient(String s, Integer integer);

    protected abstract void validateResults();

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
