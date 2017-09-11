package pt.uminho.haslab.smcoprocessors.comunication;

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
        LOG.debug("Starting server " + bindingAddress + ":" + bindingPort);
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
        LOG.debug(this.bindingPort + " is shuting down relay server");

        for (Client c : clients) {
            LOG.debug(this.bindingPort + " is going to check client status");
            while (c.isRunning()) {
                LOG.debug("Clients not closed");
                Thread.sleep(500);
            }
        }
        running = false;

        //Client to trigger loop to  force thread to verify running state and exit
        RelayClient closeClient = new RelayClient(bindingPort, bindingAddress, bindingPort);
        closeClient.connectToTarget();
        closeClient.start();
        closeClient.shutdown();

        LOG.debug(this.bindingPort + " server has running state of " + running);
        //Wait for main loop to exit and close server socket.
        mainLoopClosed.await();
        LOG.debug("All clients closed");
        serverSocket.close();

    }

    public void forceShutdown() throws IOException {
        running = false;
        serverSocket.close();
    }

    public void startServer() {
        this.start();
        broker.relayStarted();
        LOG.debug(this.bindingPort + " is starting relay server");

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
        } catch (IOException ex) {
            LOG.debug(ex);
            throw new IllegalStateException(ex);
        }
    }
}
