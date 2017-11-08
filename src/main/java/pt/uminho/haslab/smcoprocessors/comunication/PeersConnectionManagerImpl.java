package pt.uminho.haslab.smcoprocessors.comunication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PeersConnectionManagerImpl implements PeersConnectionManager {
	private static final Log LOG = LogFactory
			.getLog(PeersConnectionManagerImpl.class.getName());

	private Map<String, RelayClient> connectionToClients;
	private int playerBindPort;

	private boolean shutdownClients;

	public PeersConnectionManagerImpl(int playerBindPort) {
		this.playerBindPort = playerBindPort;
		connectionToClients = new HashMap<String, RelayClient>();
		shutdownClients = false;
	}

	public synchronized RelayClient getRelayClient(String host, int port) {
		String key = host + ":" + port;
		if (connectionToClients.containsKey(key)) {
			return connectionToClients.get(key);
		} else {
			RelayClient client = new RelayClient(playerBindPort, host, port);
			try {
				client.connectToTarget();
			} catch (InterruptedException e) {
				LOG.error(e);
				throw new IllegalStateException(e);
			} catch (IOException e) {
				LOG.error(e);
				throw new IllegalStateException(e);
			}
			client.start();
			connectionToClients.put(key, client);
			return client;
		}
	}

	public synchronized void shutdownClients() throws IOException,
			InterruptedException {
		if (!shutdownClients) {
			LOG.debug("Going to shutdown client");
			for (RelayClient cli : connectionToClients.values()) {
				cli.shutdown();
				cli.join();
			}
			shutdownClients = true;
		}

	}
}
