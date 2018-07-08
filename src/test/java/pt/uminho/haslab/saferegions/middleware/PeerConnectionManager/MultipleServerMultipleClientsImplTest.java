package pt.uminho.haslab.saferegions.middleware.PeerConnectionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.PeersConnectionManager;
import pt.uminho.haslab.saferegions.comunication.PeersConnectionManagerImpl;
import pt.uminho.haslab.saferegions.comunication.RelayClient;
import pt.uminho.haslab.saferegions.helpers.RegionServer;
import pt.uminho.haslab.saferegions.helpers.TestMessageBroker;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public class MultipleServerMultipleClientsImplTest
        extends
        MultipleServersMultipleClients {

    private static final Log LOG = LogFactory
            .getLog(CorrectSingleServerMultipleClientsTest.class.getName());
    private final Map<String, MessageBrokerImpl> messageBrokers;
    private final List<List<byte[]>> messagesToSend;
    /**
     * Index to keep track of the current client being created. Each Client is
     * composed of a binding address and port specified by a position i in the
     * clientConnectionTargetAddress list and clientConnectionTargetPort list.
     */
    private int currentClientBeingCreated;

    /**
     * To understand how this test works, it is important to notice that the
     * number of clients is different from the number of servers. Furthermore a
     * Server can receive messages from multiple clients, but a client can only
     * send to a single server. To verify that the test was correct, check that
     * the received messages on each server contains the messages sent from
     * every client. Another way to verify this is to go client by client and
     * check that the messages sent are a subset of the messages received by the
     * server.
     **/
    public MultipleServerMultipleClientsImplTest(
            List<String> serverBindingAddressees,
            List<Integer> serverBindingPorts,
            List<String> clientConnectionTargetAddress,
            List<Integer> clientConnectionTargetPort,
            List<List<byte[]>> messagesToSend) {
        super(serverBindingAddressees, serverBindingPorts,
                clientConnectionTargetAddress, clientConnectionTargetPort);
        messageBrokers = new HashMap<String, MessageBrokerImpl>();
        this.messagesToSend = messagesToSend;
        currentClientBeingCreated = 0;
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator
                .PeerConnectionManagerMultipleServerTestValueGenerator(
                        NSERVERS, NCLIENTS, NMESSAGES);
    }

    protected RegionServer createServer(String bindingAddress,
                                        Integer bindingPort) throws IOException, InterruptedException {
        MessageBrokerImpl mb = new MessageBrokerImpl(bindingAddress,
                bindingPort);
        PlayerServer server = new PlayerServer(bindingAddress, bindingPort, mb);
        String serverKey = bindingAddress + ":" + bindingPort;
        this.messageBrokers.put(serverKey, mb);

        return server;
    }

    protected RegionServer createClient(String bindingAddress,
                                        Integer bindingPort) {
        PlayerClient client = new PlayerClient(currentClientBeingCreated,
                bindingAddress, bindingPort,
                messagesToSend.get(currentClientBeingCreated));
        currentClientBeingCreated += 1;
        return client;
    }

    protected void validateResults() {

        for (int i = 0; i < NCLIENTS; i++) {
            String serverBindingAddress = clientConnectionTargetAddress.get(i);
            int serverBindingPort = clientConnectionTargetPort.get(i);
            String key = serverBindingAddress + ":" + serverBindingPort;

            List<byte[]> clientMessagesToSend = messagesToSend.get(i);
            MessageBrokerImpl mbi = messageBrokers.get(key);
            List<byte[]> mbiMessagesReceived = mbi.getReceivedMessages();

            int matches = 0;

            for (byte[] msg : clientMessagesToSend) {

                for (byte[] bmsg : mbiMessagesReceived) {
                    if (Arrays.equals(msg, bmsg)) {
                        matches += 1;
                        break;
                    }
                }
            }
            LOG.debug("Comparing number of messages "
                    + clientMessagesToSend.size() + "-" + matches);
            assertEquals(clientMessagesToSend.size(), matches);
        }
    }

    private class MessageBrokerImpl extends TestMessageBroker {

        private final String serverBindingAddress;
        private final int serverBindingPort;
        private final List<byte[]> receivedMessages;

        MessageBrokerImpl(String serverBindingAddress, int serverBindingPort) {
            this.serverBindingAddress = serverBindingAddress;
            this.serverBindingPort = serverBindingPort;
            receivedMessages = new ArrayList<byte[]>();
        }

        public void receiveTestMessage(byte[] message) {
            LOG.debug("ReceivedMessage");
            receivedMessages.add(message);
            allMessagesReceived.countDown();
        }

        public String getServerBindingAddress() {
            return serverBindingAddress;
        }

        public int getServerBindingPort() {
            return serverBindingPort;
        }

        public List<byte[]> getReceivedMessages() {
            return receivedMessages;
        }
    }

    private class PlayerServer extends AbsPlayerServer {

        PlayerServer(String bindingAddress, int bindingPort,
                     MessageBroker broker) throws IOException {
            super(bindingAddress, bindingPort, broker);
        }

        @Override
        public void run() {
            LOG.debug("Waiting for server to start");
            server.startServer();
            try {
                this.broker.waitRelayStart();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
            LOG.debug("Server has started");
            allServersStarted.countDown();
            try {
                server.join();
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
        }
    }

    private class PlayerClient extends AbsPlayerClient {

        private final List<byte[]> messagesToSend;
        private final PeersConnectionManager connectionManager;

        PlayerClient(int playerID, String ip, int port,
                     List<byte[]> messagesToSend) {
            super(playerID, ip, port);
            this.messagesToSend = messagesToSend;
            connectionManager = new PeersConnectionManagerImpl(0);
        }

        @Override
        public void run() {
            RelayClient client = connectionManager.getRelayClient(ip, port);
            try {
                LOG.debug("Send Messages " + messagesToSend.size());
                for (byte[] message : messagesToSend) {
                    client.sendTestMessage(message);
                }
                LOG.debug("Messages Sent");
                allClientsStarted.countDown();
                connectionManager.shutdownClients();
            } catch (IOException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            } catch (InterruptedException e) {
                LOG.error(e.getLocalizedMessage());
                throw new IllegalStateException(e);
            }
        }
    }

}
