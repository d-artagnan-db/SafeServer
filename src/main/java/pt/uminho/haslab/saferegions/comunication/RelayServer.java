package pt.uminho.haslab.saferegions.comunication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/*Class that handles the server side of the relay*/
public class RelayServer extends Thread {

    private static final Log LOG = LogFactory.getLog(RelayServer.class
            .getName());

    private final int bindingPort;
    private final String bindingAddress;

    private final MessageBroker broker;

    private final ServerSocket serverSocket;

    private final List<Client> clients;
    private boolean running;

    private long clientsReceived;

    private CountDownLatch mainLoopClosed;

    public RelayServer(final String bindingAddress, final int bindingPort,
                       final MessageBroker broker) throws IOException {
        this.bindingPort = bindingPort;
        this.bindingAddress = bindingAddress;
        this.broker = broker;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting server " + bindingAddress + ":" + bindingPort);
        }
        clientsReceived = 0;
        clients = new ArrayList<Client>();
        serverSocket = new ServerSocket(bindingPort);
        running = true;
        mainLoopClosed = new CountDownLatch(1);

    }

    public int getBindingPort() {
        return this.bindingPort;
    }

    public void shutdown() throws InterruptedException, IOException {
        LOG.debug(this.bindingPort + " is shutting down relay server");

        for (Client c : clients) {
            LOG.debug(this.bindingPort + " is going to check client status");
            while (c.isRunning()) {
                LOG.debug("Clients not closed");
                Thread.sleep(1000);
            }
        }
        running = false;

        voidClient();
        LOG.debug(this.bindingPort + " server has running state of " + running);
        // Wait for main loop to exit and close server socket.
        mainLoopClosed.await();
        LOG.debug("All clients closed");
        serverSocket.close();
        LOG.debug("Exiting Relay Server close");

    }

    /*
     * Client to trigger loop to force thread to verify running state and close
     * server
     */
    private void voidClient() throws IOException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Void client is triggered to stop server");
        }
        RelayClient closeClient = new RelayClient(bindingPort, bindingAddress,
                bindingPort);
        closeClient.connectToTarget();
        closeClient.start();
        closeClient.shutdown();

    }

    public void forceShutdown() throws IOException, InterruptedException {
        running = false;
        voidClient();
        mainLoopClosed.await();
        LOG.debug("All clients closed");
        serverSocket.close();
        LOG.debug("Exiting Relay Server close");
    }

    public void startServer() {
        this.start();
        broker.relayStarted();
        if (LOG.isDebugEnabled()) {
            LOG.debug(this.bindingPort + " is starting relay server");
        }

    }

    public long getClientsReceived() {
        return clientsReceived;
    }

    @Override
    public void run() {
        try {
            while (running) {

                Socket socketClient = serverSocket.accept();
                Client client = new Client(socketClient, broker);
                client.start();
                this.clients.add(client);
                clientsReceived += 1;
            }

            mainLoopClosed.countDown();
            LOG.debug(this.bindingPort + " closed Relay Server main loop");

        } catch (IOException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }
}
