package pt.uminho.haslab.smcoprocessors.middleware.PeerConnectionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RelayClient;
import pt.uminho.haslab.smcoprocessors.TestMessageBroker;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public class CorrectSingleServerMultipleClientsTest extends SingleServerMultipleClients {

    private static final Log LOG = LogFactory.getLog(CorrectSingleServerMultipleClientsTest.class.getName());
    private final String serverIp;
    private final int serverPort;
    private final byte[] messageToSend;
    private final List<byte[]> receivedMessages;
    private final CountDownLatch allClientsSent;
    public CorrectSingleServerMultipleClientsTest(String serverIp, int serverPort, byte[] messagesToSend) {
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.messageToSend = messagesToSend;
        this.receivedMessages = new ArrayList<byte[]>();
        allClientsSent = new CountDownLatch(NCLIENTS);
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.PeerConnectionManagerSingleServerTestValueGenerator();
    }

    protected void validateResults() {
        for (byte[] receivedMessage : receivedMessages) {
            assertArrayEquals(messageToSend, receivedMessage);
        }
    }

    protected RegionServer createClient(int i) {
        return new PlayerClient(i, serverIp, serverPort);
    }

    protected RegionServer createServer() throws IOException, InterruptedException {
        MessageBrokerImpl mb = new MessageBrokerImpl();
        PlayerServer server = new PlayerServer(serverIp, serverPort, mb);
        server.startRegionServer();
        LOG.debug("Waiting for server to start");
        mb.waitRelayStart();
        LOG.debug("Server has started");
        return server;
    }

    private class MessageBrokerImpl extends TestMessageBroker {

        public void receiveTestMessage(byte[] message) {
            synchronized (this) {
                receivedMessages.add(message);
            }
        }
    }

    private class PlayerServer extends AbsPlayerServer {

        PlayerServer(String bindingAddress, int bindingPort, MessageBroker broker) throws IOException {
            super(bindingAddress, bindingPort, broker);
        }

        @Override
        public void run() {
            server.startServer();
            LOG.debug("Server has started");
            try {
                server.join();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
        }
    }


    private class PlayerClient extends AbsPlayerClient {

        PlayerClient(int playerID, String ip, int port) {
            super(playerID, ip, port);
        }

        @Override
        public void run() {
            RelayClient client = clientPeerConnectionManager.getRelayClient(ip, port);
            try {
                client.sendTestMessage(messageToSend);
                allClientsSent.countDown();
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }

            try {
                allClientsSent.await();
                LOG.debug("Going to close client connections");
                clientPeerConnectionManager.shutdownClients();
                LOG.debug("Client connections closed");
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
        }
    }
}
