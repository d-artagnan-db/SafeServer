package pt.uminho.haslab.saferegions.comunication;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.IntResultsMessage;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;

import java.io.*;
import java.net.Socket;

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
		switch (type) {

			case 0 : {
				String msg = "Share messages no longer supported";
				LOG.error(msg);
				throw new IllegalStateException(msg);
			}
			case 1 : {
				new ResultsHandler(message).handle();
				break;
			}
			case 2 : {
				new FilterIndexHandler(message).handle();
				break;
			}
			case 3 : {
				new BatchShareHandler(message).handle();
				break;
			}
			case 4 : {
				//LOG.debug("Client received Int Batch Share Handler ");
				new IntBatchShareHandler(message).handle();
				break;
			}
			case 5 : {
               // LOG.debug("Client received Int results handler ");
                new IntResultsHandler(message).handle();
				break;
			}
			// Message issued to close connection.
			case 99 : {
				LOG.debug("Received message to close " + clientSocket.getPort());
				toClose = true;
				break;
			}
			// Message used for UnitTests
			case 999 : {
				new TestMessageHandler(message).handle();
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
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} finally {
			try {
				assert in != null;
				in.close();
			} catch (IOException ex) {
				LOG.error(ex);
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
				LOG.error(ex);
				throw new IllegalStateException(ex);
			}

		}

	}


	private class IntResultsHandler extends MessageHandler {

		public IntResultsHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
			try {
				IntResultsMessage message = IntResultsMessage.parseFrom(msg);
				broker.receiveProtocolResults(message);
			} catch (InvalidProtocolBufferException ex) {
				LOG.error(ex);
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
				LOG.error(ex);
				throw new IllegalStateException(ex);
			}
		}
	}

	private class BatchShareHandler extends MessageHandler {

		public BatchShareHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
			try {
				BatchShareMessage message = BatchShareMessage.parseFrom(msg);
				broker.receiveBatchMessage(message);

			} catch (InvalidProtocolBufferException e) {
				LOG.error(e);
				throw new IllegalStateException(e);
			}
		}
	}

	private class IntBatchShareHandler extends MessageHandler {

		public IntBatchShareHandler(byte[] msg) {
			super(msg);
		}

		@Override
		public void handle() {
				CIntBatchShareMessage message = CIntBatchShareMessage.parseFrom(msg);
				broker.receiveBatchMessage(message);
		}
	}



	private class TestMessageHandler extends MessageHandler {

		public TestMessageHandler(byte[] msg) {
			super(msg);
		}

		public void handle() {
			broker.receiveTestMessage(msg);
		}
	}
}