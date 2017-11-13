package pt.uminho.haslab.saferegions.middleware.relay;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.saferegions.comunication.*;
import pt.uminho.haslab.saferegions.discovery.DiscoveryService;
import pt.uminho.haslab.saferegions.discovery.FailedRegionDiscovery;
import pt.uminho.haslab.saferegions.discovery.RedisDiscoveryService;
import pt.uminho.haslab.saferegions.discovery.RegionLocation;
import pt.uminho.haslab.saferegions.helpers.RegionServer;
import pt.uminho.haslab.saferegions.helpers.TestMessageBroker;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AllToAllMessageExchangeDistributedClusterTest
		extends
			MessageExchangeDistributedClusterTest {

	private static final Log LOG = LogFactory
			.getLog(AllToAllMessageExchangeDistributedClusterTest.class
					.getName());
	private static final byte[] regionIndentifier = new BigInteger("0")
			.toByteArray();

	public AllToAllMessageExchangeDistributedClusterTest(
			List<String> bindingAddress, List<Integer> bindingPort,
			List<List<byte[]>> messagesToSend,
			List<List<BigInteger>> requestIdentifier) {
		super(bindingAddress, bindingPort, messagesToSend, requestIdentifier);
		if (NREGIONS % 2 == 0) {
			/**
			 * In this test we want every region to connect to every other
			 * region and process a different region. Thus each region will have
			 * two connections per request and the number of regions must be a
			 * odd number, so every region can connect with every other region
			 * with the RedisDiscoveryService and PeerConnectionManager. This
			 * test simulates what could happen in a correct deployment, with
			 * every Region connection to just other two Regions.
			 * 
			 * The test works as follows, given a Number of regions multiple of
			 * 3 for instance 6. [a|b|c|d|e] -> Array with regions, named from a
			 * to f. #NREGIONS = 6. The first region a, will connect to b and c
			 * for request X, and a will connect to d and e for request Y. The
			 * second region b, will connect to c and d for Request Z and region
			 * b will connect to e and a for a request K. As can be seen by this
			 * example, by following a modular ring pattern, each region can
			 * connect to every other region.
			 * 
			 * */
			String msg = "Number of Regions must be a multiple of 3. A MPC protocol requires three regions to process a request.";
			throw new IllegalStateException(msg);
		}
	}

	@Parameterized.Parameters
	public static Collection nbitsValues() {
		return ValuesGenerator.AllToAllMessageExchangeDistributedCluster(
				NREGIONS, NMESSAGES);
	}

	protected void validateResults() {

		for (RegionServer rs : regionServers) {
			RegionServerImpl rsi = (RegionServerImpl) rs;
			MessageBrokerImpl mbi = (MessageBrokerImpl) rsi.getMessageBroker();

			// In this test it is assumed that everyone sends the same message,
			// thus everyone should receive the same.
			List<byte[]> clientMessagesToSend = this.messagesToSend.get(0);
			int ngroups = NREGIONS / 3;
			int messagesThatRegionShouldReceive = NMESSAGES * ngroups * 2;

			assertEquals(messagesThatRegionShouldReceive, mbi
					.getReceivedMessages().size());

			for (int i = 0; i < clientMessagesToSend.size(); i++) {
				boolean found = false;
				int nMatches = 0;
				for (int j = 0; j < messagesThatRegionShouldReceive; j++) {

					if (ArrayUtils.isEquals(clientMessagesToSend.get(i), mbi
							.getReceivedMessages().get(j))) {
						found |= true;
						nMatches += 1;
					}
				}
				assertEquals(true, found);
				assertEquals(ngroups * 2, nMatches);
			}

		}

	}

	protected RegionServer createRegionServer(int playerID, int index,
			List<String> bindingAddress, List<Integer> bindingPort,
			List<byte[]> messagesToSend, List<BigInteger> requestIdentifier)
			throws IOException {
		return new RegionServerImpl(playerID, index, bindingAddress,
				bindingPort, messagesToSend, requestIdentifier);
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

	protected class RegionServerImpl extends Thread implements RegionServer {

		private final int playerID;
		private final int index;
		private final List<String> bindingAddress;
		private final List<Integer> bindingPort;
		private final List<byte[]> messagesToSend;
		private final List<BigInteger> requestIdentifiers;
		private final PeersConnectionManagerImpl connectionManager;
		private DiscoveryService discoveryService;
		private boolean runStatus;
		private MessageBroker mb;

		RegionServerImpl(int playerID, int index, List<String> bindingAddress,
				List<Integer> bindingPort, List<byte[]> messagesToSend,
				List<BigInteger> requestIdentifier) {
			this.playerID = playerID;
			this.index = index;
			this.bindingAddress = bindingAddress;
			this.bindingPort = bindingPort;
			this.messagesToSend = messagesToSend;
			this.requestIdentifiers = requestIdentifier;
			this.connectionManager = new PeersConnectionManagerImpl(
					bindingPort.get(index));
			runStatus = false;

		}

		public void startRegionServer() {
			this.start();

		}

		public void stopRegionServer() throws IOException, InterruptedException {
			this.join();

		}

		public MessageBroker getMessageBroker() {
			return this.mb;
		}

		public boolean getRunStatus() {
			return runStatus;
		}

		@Override
		public void run() {
			runStatus = true;
			LOG.debug("RegionServerIndex is " + index);
			String serverBindingAddress = bindingAddress.get(index);
			int serverBindingPort = bindingPort.get(index);
			mb = new MessageBrokerImpl(serverBindingAddress, serverBindingPort);
			RelayServer server;

			try {
				server = new RelayServer(serverBindingAddress,
						serverBindingPort, mb);
				server.startServer();
				mb.waitRelayStart();

			} catch (IOException e) {
				LOG.debug("Error on binding to address " + serverBindingAddress
						+ ":" + serverBindingPort);
				LOG.error(e.getLocalizedMessage());
				throw new IllegalStateException(e);
			} catch (InterruptedException e) {
				LOG.error(e.getLocalizedMessage());
				throw new IllegalStateException(e);
			}
			// Wait for servers in every region to start.
			serversStarted.countDown();
			try {
				serversStarted.await();
			} catch (InterruptedException e) {
				LOG.error(e.getLocalizedMessage());
				throw new IllegalStateException(e);
			}

			// Initiate connection to the discovery Service and register server.
			discoveryService = new RedisDiscoveryService("localhost", playerID,
					serverBindingAddress, serverBindingPort,
					DISC_SERVICE_SLEEP_TIME, DISC_SERVICE_INC_TIME,
					DISC_SERVICE_RETRIES);

			// Create Client connections and send messages.
			for (BigInteger reqIdent : requestIdentifiers) {
				LOG.debug("RegionServer " + playerID
						+ " starting Requests Loop with request " + reqIdent);
				RequestIdentifier uniqueIdent = new RequestIdentifier(
						reqIdent.toByteArray(), regionIndentifier);
				discoveryService.registerRegion(uniqueIdent);
				try {
					LOG.debug("RegionServer " + playerID
							+ " discovering regions");

					List<RegionLocation> locations = discoveryService
							.discoverRegions(uniqueIdent);

					for (RegionLocation location : locations) {
						RelayClient client = connectionManager.getRelayClient(
								location.getIp(), location.getPort());

						for (byte[] message : messagesToSend) {
							client.sendTestMessage(message);
							totalMessagesCounter.countDown();
						}

					}
				} catch (FailedRegionDiscovery failedRegionDiscovery) {
					LOG.error(failedRegionDiscovery.getLocalizedMessage());
					throw new IllegalStateException(failedRegionDiscovery);
				} catch (IOException e) {
					LOG.error(e.getLocalizedMessage());
					throw new IllegalStateException(e);
				}
			}

			try {
				totalMessagesCounter.await();
				LOG.debug("Closing client connections");
				connectionManager.shutdownClients();
				LOG.debug("Client connections closed");
				LOG.debug("Closing server");
				server.shutdown();

			} catch (InterruptedException e) {
				LOG.error(e.getLocalizedMessage());
				throw new IllegalStateException(e);
			} catch (IOException e) {
				LOG.error(e.getLocalizedMessage());
				throw new IllegalStateException(e);
			}
			runStatus = false;
		}
	}
}
