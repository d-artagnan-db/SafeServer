package pt.uminho.haslab.smcoprocessors.CMiddleware;

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

    private final long messagesReceived;

    private final ServerSocket serverSocket;

    private final List<Client> clients;
    private boolean running;

    private long clientsReceived;

    private final CountDownLatch playersConnected;

    public RelayServer(final String bindingAdress, final int bindingPort,
                       final MessageBroker broker) throws IOException {
        this.bindingPort = bindingPort;
        this.bindingAddress = bindingAdress;
        this.broker = broker;
        LOG.debug("Starting server " + bindingPort);
        messagesReceived = 0;
        clientsReceived = 0;
        clients = new ArrayList<Client>();
        serverSocket = new ServerSocket(bindingPort);

        playersConnected = new CountDownLatch(2);
        running = true;

    }

    public int getBindingPort() {
        return this.bindingPort;
    }

    public void shutdown() throws InterruptedException, IOException {
        LOG.debug(this.bindingPort + " is shuting down relay server");

        for (Client c : clients) {
            LOG.debug(this.bindingPort + " is going to check client status");
            while (c.isRunning()) {
                Thread.sleep(500);
            }
        }
        LOG.debug(this.bindingPort + " server has running state of " + running);
        while (running) {
            Thread.sleep(500);
        }
        LOG.debug("All clients closed");
        serverSocket.close();

    }

    public void forceShutdown() throws IOException {
        running = false;
        serverSocket.close();
    }

    public void startServer() {
        LOG.debug(this.bindingPort + " is starting relay server");
        this.start();
    }

    public long getClientsReceived() {
        return clientsReceived;
    }

    @Override
    public void run() {
        try {
            while (running) {
                if (clientsReceived == 2) {
                    running = false;
                } else {
                    Socket socketClient = serverSocket.accept();
                    Client client = new Client(socketClient, broker);
                    client.start();
                    this.clients.add(client);
                    clientsReceived += 1;
                    playersConnected.countDown();

                }
            }
        } catch (IOException ex) {
            LOG.debug(ex);
            throw new IllegalStateException(ex);
        }

    }

    public void waitPlayersToConnect() throws InterruptedException {
        this.playersConnected.await();

    }

}
