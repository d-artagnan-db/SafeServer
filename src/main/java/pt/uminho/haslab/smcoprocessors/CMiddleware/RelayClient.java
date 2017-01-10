package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;
import pt.uminho.haslab.protocommunication.Search.Shutdown;

/**
 * Class that handles the connection to a client by the relay
 */
public class RelayClient extends Thread {

	private static final Log LOG = LogFactory.getLog(RelayClient.class
			.getName());

	/**
	 * This two metrics are used for statistics and to check if every message
	 * request to send to be sent by the async channel is actually delivered.
	 * For test cases this is important because we are expecting that every
	 * message is sent and only than can the connections be closed.
	 */
	private final AtomicLong messagesSent;
	private final AtomicLong messagesAskedToSend;

	private final int targetPort;
	private final String targetAddress;
	private final int bindingPort;

	private Socket socket;
	private DataOutputStream out;
	private DataInputStream in;
	private boolean running;

	public RelayClient(final int bindingPort, final String targetAddress,
			final int targetPort) {

		this.bindingPort = bindingPort;
		messagesSent = new AtomicLong();
		messagesAskedToSend = new AtomicLong();

		this.targetPort = targetPort;
		this.targetAddress = targetAddress;
		running = true;

	}

	public void sendToClient(int type, byte[] msg) throws IOException {

		// System.out.println(this.bindingPort +
		// " asked to send message of type "+ type + " with size "+ msg.length +
		// " to "+targetPort);
		out.writeInt(msg.length);
		out.writeInt(type);
		out.write(msg);
		out.flush();

	}

	public void sendMessage(ShareMessage msg) throws IOException {

		messagesAskedToSend.addAndGet(1);
		sendToClient(0, msg.toByteArray());
	}

	public void sendProtocolResults(ResultsMessage msg) throws IOException {
		messagesAskedToSend.addAndGet(1);
		sendToClient(1, msg.toByteArray());
	}

	public void sendFilteredIndexes(FilterIndexMessage msg) throws IOException {
		messagesAskedToSend.addAndGet(1);
		sendToClient(2, msg.toByteArray());
	}

	public void sendShutdown() throws IOException {
		Shutdown shutdown = Shutdown.newBuilder().build();
		sendToClient(99, shutdown.toByteArray());
	}

	public void connectToTarget() throws InterruptedException, IOException {
		/*
		 * System.out.println(this.bindingPort+"Going to connect to " +
		 * targetAddress + ":" + targetPort);
		 */
		LOG.info((this.bindingPort + " is going to connect to " + targetAddress
				+ ":" + targetPort));
		socket = new Socket(targetAddress, targetPort);
		out = new DataOutputStream(new BufferedOutputStream(
				socket.getOutputStream()));
		in = new DataInputStream(new BufferedInputStream(
				socket.getInputStream()));
	}

	public void shutdown() throws InterruptedException, IOException {
		/*
		 * LOG.debug(targetAddress + ":" + targetPort +
		 * " is shutting down channel");
		 */
		LOG.debug(this.bindingPort + " is going to shutdown connection to "
				+ targetAddress + ":" + targetPort);
		LOG.debug(this.bindingPort + " asked to send "
				+ this.messagesAskedToSend + " messages and actualy sent "
				+ this.messagesSent);
		while (messagesSent.get() != messagesAskedToSend.get()) {
			LOG.debug(this.bindingPort + " asked to send "
					+ this.messagesAskedToSend + " messages and actualy sent "
					+ this.messagesSent);
			Thread.sleep(5000);
		}

		running = false;
		sendShutdown();
		// socket.close();
		LOG.debug(this.bindingPort + " is shutting down now " + targetAddress
				+ ":" + targetPort);

	}

	@Override
	public void run() {
		LOG.debug(this.bindingPort + " is going to start relay client thread");
		while (running) {
			try {
				int vRead = in.readInt();
				if (vRead != -99) {
					messagesSent.addAndGet(1);
				}
			} catch (IOException ex) {
				LOG.debug("Error on closing socket that was waiting " + ex);
			}

		}
		try {
			socket.close();
		} catch (IOException ex) {
			LOG.debug("socket could not be closed" + ex);
		}

	}

}
