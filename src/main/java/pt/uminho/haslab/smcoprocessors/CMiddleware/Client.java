package pt.uminho.haslab.smcoprocessors.CMiddleware;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;

public class Client extends Thread {

	private static final Log LOG = LogFactory.getLog(Client.class.getName());

	private final Socket clientSocket;

	private final MessageBroker broker;

	private boolean running;

	private boolean toClose;

	public Client(Socket clientSocket, MessageBroker broker) {
		this.clientSocket = clientSocket;
		this.broker = broker;
		running = true;
	}
	private DataInputStream getInStream() throws IOException {
		return new DataInputStream(new BufferedInputStream(
				this.clientSocket.getInputStream()));
	}

	private DataOutputStream getOutStream() throws IOException {
		return new DataOutputStream(new BufferedOutputStream(
				this.clientSocket.getOutputStream()));

	}

	private void handleMessage(int type, byte[] message) throws IOException {
		LOG.debug("Handle message with type " + type + " and size "
				+ message.length);
		switch (type) {

			case 0 : {
				new ShareHandler(message).handle();
				break;
			}
			case 1 : {
				new ResultsHandler(message).handle();
				break;
			}
			case 2 : {
				new FilterIndexHandler(message).handle();
				break;
			}
			case 99 : {
				toClose = true;
				break;
			}
		}

	}

	public void close() throws IOException {
		running = false;
		clientSocket.close();
	}

	public boolean isRunning() {
		return running;
	}

	@Override
	public void run() {
		DataInputStream in = null;
		DataOutputStream out = null;
		try {
			in = getInStream();
			out = getOutStream();

			while (running) {
				int messageSize = in.readInt();
				int messageType = in.readInt();
				byte[] message = new byte[messageSize];
				in.readFully(message);

				handleMessage(messageType, message);

				if (toClose) {
					out.writeInt(-99);
					out.flush();
					close();
				} else {
					out.writeInt(0);
					out.flush();
				}

			}
		} catch (IOException ex) {
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		} finally {
			try {
				in.close();
			} catch (IOException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}
		}

	}

	private abstract class MessageHandler {

		protected final byte[] msg;

		public MessageHandler(byte[] msg) {
			this.msg = msg;
		}

		public abstract void handle();
	}

	private class ShareHandler extends MessageHandler {

		public ShareHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
			try {
				ShareMessage message = ShareMessage.parseFrom(msg);
				broker.receiveMessage(message);
			} catch (InvalidProtocolBufferException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);

			}

		}

	}

	private class ResultsHandler extends MessageHandler {

		public ResultsHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
			try {
				ResultsMessage message = ResultsMessage.parseFrom(msg);
				broker.receiveProtocolResults(message);
			} catch (InvalidProtocolBufferException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}

		}

	}

	private class FilterIndexHandler extends MessageHandler {

		public FilterIndexHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
			try {
				FilterIndexMessage message = FilterIndexMessage.parseFrom(msg);
				broker.receiveFilterIndex(message);
			} catch (InvalidProtocolBufferException ex) {
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}
		}

	}
}