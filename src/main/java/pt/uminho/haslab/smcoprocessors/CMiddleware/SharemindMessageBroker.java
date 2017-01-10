package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;

public class SharemindMessageBroker implements MessageBroker {

	private final Lock lock;

	// Locks for protocol messages exchange
	private final RequestsLocks protocolMessagesLocks;

	// Locks for protocol results
	private final RequestsLocks protocolResultsLocks;

	private final RequestsLocks filterIndexLocks;

	// protocol messages received
	private final Map<RequestIdentifier, Queue<ShareMessage>> messagesReceived;

	// protocol resuts messages received
	private final Map<RequestIdentifier, Queue<ResultsMessage>> protocolResults;

	private final Map<RequestIdentifier, FilterIndexMessage> filterIndex;

	private final CountDownLatch relayStarted;

	public SharemindMessageBroker() {
		protocolMessagesLocks = new RequestsLocks();
		protocolResultsLocks = new RequestsLocks();
		filterIndexLocks = new RequestsLocks();

		relayStarted = new CountDownLatch(1);
		lock = new ReentrantLock();

		messagesReceived = new ConcurrentHashMap<RequestIdentifier, Queue<ShareMessage>>();
		protocolResults = new ConcurrentHashMap<RequestIdentifier, Queue<ResultsMessage>>();
		filterIndex = new ConcurrentHashMap<RequestIdentifier, FilterIndexMessage>();

	}

	public void receiveMessage(ShareMessage message) {
		lock.lock();
		RequestIdentifier requestID = new RequestIdentifier(message
				.getRequestID().toByteArray(), message.getRegionID()
				.toByteArray());
		try {
			protocolMessagesLocks.lockOnRequest(requestID);
			lock.unlock();

			if (messagesReceived.containsKey(requestID)) {
				messagesReceived.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<ShareMessage>();
				values.add(message);
				messagesReceived.put(requestID, values);
			}

			protocolMessagesLocks.signalToRead(requestID);

		} finally {
			protocolMessagesLocks.unlockOnRequest(requestID);
		}

	}

	@Override
	public void receiveProtocolResults(ResultsMessage message) {
		lock.lock();
		RequestIdentifier requestID = new RequestIdentifier(message
				.getRequestID().toByteArray(), message.getRegionID()
				.toByteArray());
		try {
			protocolResultsLocks.lockOnRequest(requestID);
			lock.unlock();
			if (protocolResults.containsKey(requestID)) {
				protocolResults.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<ResultsMessage>();
				values.add(message);
				protocolResults.put(requestID, values);
			}
			protocolResultsLocks.signalToRead(requestID);
		} finally {
			protocolResultsLocks.unlockOnRequest(requestID);
		}
	}

	@Override
	public void receiveFilterIndex(FilterIndexMessage message) {
		lock.lock();
		RequestIdentifier requestID = new RequestIdentifier(message
				.getRequestID().toByteArray(), message.getRegionID()
				.toByteArray());
		try {
			filterIndexLocks.lockOnRequest(requestID);
			lock.unlock();
			filterIndex.put(requestID, message);
			filterIndexLocks.signalToRead(requestID);

		} finally {
			filterIndexLocks.unlockOnRequest(requestID);
		}

	}

	@Override
	public Queue<ShareMessage> getReceivedMessages(RequestIdentifier requestId) {
		lock.lock();

		try {

			protocolMessagesLocks.lockOnRequest(requestId);
			lock.unlock();

			// While there arent messages for this request wait.
			while (!messagesReceived.containsKey(requestId)) {
				protocolMessagesLocks.awaitForWrite(requestId);
			}

			return messagesReceived.get(requestId);

		} catch (InterruptedException ex) {
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

	@Override
	public Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID) {
		lock.lock();
		try {
			protocolResultsLocks.lockOnRequest(requestID);
			lock.unlock();
			/**
			 * Wait while protocol results do not arrive. Only two results
			 * should arrive, one from each of the remaining players.
			 */

			while (!(protocolResults.containsKey(requestID) && protocolResults
					.get(requestID).size() == 2)) {
				protocolResultsLocks.awaitForWrite(requestID);
			}
			return protocolResults.get(requestID);

		} catch (InterruptedException ex) {
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

	@Override
	public FilterIndexMessage getFilterIndexes(RequestIdentifier requestID) {
		lock.lock();

		try {
			filterIndexLocks.lockOnRequest(requestID);
			lock.unlock();
			// While there arent messages for this request wait.
			while (!filterIndex.containsKey(requestID)) {
				filterIndexLocks.awaitForWrite(requestID);
			}

			return filterIndex.get(requestID);

		} catch (InterruptedException ex) {

			throw new IllegalArgumentException(ex.getMessage());
		}

	}

	@Override
	public void relayStarted() {
		relayStarted.countDown();
	}

	@Override
	public void waitRelayStart() throws InterruptedException {
		relayStarted.await();

	}
	@Override
	public void allMessagesRead(RequestIdentifier requestID) {

		protocolMessagesLocks.removeLock(requestID);
		messagesReceived.remove(requestID);

	}

	@Override
	public void readMessages(RequestIdentifier requestId) {
		protocolMessagesLocks.unlockOnRequest(requestId);
	}

	@Override
	public void waitNewMessage(RequestIdentifier requestId)
			throws InterruptedException {
		protocolMessagesLocks.awaitForWrite(requestId);
	}

	@Override
	public void allResultsRead(RequestIdentifier requestID) {
		protocolResultsLocks.removeLock(requestID);
		protocolResults.remove(requestID);
	}

	@Override
	public void protocolResultsRead(RequestIdentifier requestID) {
		protocolResultsLocks.unlockOnRequest(requestID);
	}

	@Override
	public void allIndexesMessagesRead(RequestIdentifier requestID) {
		filterIndex.remove(requestID);
		filterIndexLocks.removeLock(requestID);
	}
	@Override
	public void indexeMessageRead(RequestIdentifier requestID) {
		filterIndexLocks.unlockOnRequest(requestID);
	}

	public int numberOfLocksMessages() {
		return this.protocolMessagesLocks.countLocks();
	}

	public int numberofOfLocksResults() {
		return this.protocolResultsLocks.countLocks();
	}

	public boolean lockExistsForMessagesOnRequest(RequestIdentifier requestID) {
		return this.protocolMessagesLocks.lockExist(requestID);

	}

	public boolean lockExistsForResultsOnRequest(RequestIdentifier requestID) {
		return this.protocolResultsLocks.lockExist(requestID);

	}

}
