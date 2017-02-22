package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;

public class IORelay implements Relay {

	static final Log LOG = LogFactory.getLog(IORelay.class.getName());

	private final RelayServer server;

	private final RelayClient firstClient;

	private final RelayClient secondClient;

	private boolean running;

	private final MessageBroker broker;

	public IORelay(String bindingAddress, int bindingPort,
			String firstTargetAddress, int firstTargetPort,
			String secondTargetAddress, int secondTargetPort,
			MessageBroker broker) throws IOException {

		server = new RelayServer(bindingAddress, bindingPort, broker);

		firstClient = new RelayClient(bindingPort, firstTargetAddress,
				firstTargetPort);
		secondClient = new RelayClient(bindingPort, secondTargetAddress,
				secondTargetPort);

		this.running = false;
		this.broker = broker;

	}

	private void connectToTarget() {

		try {
			firstClient.connectToTarget();
			secondClient.connectToTarget();
			firstClient.start();
			secondClient.start();

		} catch (InterruptedException ex) {
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		} catch (IOException ex) {
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		}

	}

	public void stopRelay() throws IOException {
		try {

			LOG.info(server.getBindingPort() + " going to stop relay");
			firstClient.shutdown();
			firstClient.join();
			secondClient.shutdown();
			secondClient.join();
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
			firstClient.shutdown();
			firstClient.join();
			secondClient.shutdown();
			secondClient.join();
			server.forceShutdown();
			LOG.debug(server.getBindingPort() + " relay force stopped");
		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

	public void stopErrorRelay() throws InterruptedException, IOException {
		firstClient.shutdown();
		secondClient.shutdown();
		server.shutdown();
	}

	public boolean isRelayRunning() {
		return running;
	}

	public void startServer() throws IOException {

		server.start();
		this.broker.relayStarted();
		this.running = true;
	}

	public void bootRelay() {

		try {

			int bp = server.getBindingPort();
			this.startServer();
			this.connectToTarget();
			LOG.info(bp + " waiting for clients to connect to server");
			this.server.waitPlayersToConnect();
			LOG.info(bp + " completed booting phase");
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

	}

	public RelayClient getTargetClient(int playerID) {
		RelayClient client = firstClient;

		if (playerID == 0) {
			client = secondClient;
		}

		return client;
	}

	@Override
	public synchronized void sendMessage(ShareMessage msg) throws IOException {
		int target = calculateDestPlayer(msg.getPlayerSource(),
				msg.getPlayerDest());

		getTargetClient(target).sendMessage(msg);
	}

	@Override
	public synchronized void sendProtocolResults(ResultsMessage msg)
			throws IOException {
		int target = calculateDestPlayer(msg.getPlayerSource(),
				msg.getPlayerDest());
		getTargetClient(target).sendProtocolResults(msg);
	}

	@Override
	public synchronized void sendFilteredIndexes(FilterIndexMessage msg)
			throws IOException {
		int target = calculateDestPlayer(msg.getPlayerSource(),
				msg.getPlayerDest());
		getTargetClient(target).sendFilteredIndexes(msg);

	}

	private int calculateDestPlayer(int playerID, int playerDest) {
		/**
		 * return 1 is connection two on NioRelay. return 0 is connection one on
		 * NioRelay. originPlayerId is the player destination from the three
		 * (0,1,2)
		 */

		switch (playerID) {
		/*
		 * if this player is 0 and wants to send to player one, then use
		 * connection two on the nio relay. Use connection one if it goes to
		 * player 2.
		 */
			case 0 :
				return playerDest == 1 ? 1 : 0;
				/*
				 * if this player is 1 and wants to send to player two, then use
				 * connection two on the nio relay. Use connection one if it
				 * goes to player 0.
				 */
			case 1 :
				return playerDest == 2 ? 1 : 0;
				/*
				 * if this player is 2 and wants to send to player zero, then
				 * use connection two on the nio relay. Use connection one if it
				 * goes to player 1.
				 */
			case 2 :
				return playerDest == 0 ? 1 : 0;
		}

		/* This does nothing, it just helps netbeans not show a warning */
		return -1;
	}

}
