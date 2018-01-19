package pt.uminho.haslab.saferegions.comunication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SharemindMessageBroker implements MessageBroker {

	private static final Log LOG = LogFactory
			.getLog(SharemindMessageBroker.class.getName());

	private final Lock lock;

	// Locks for protocol results
	private final RequestsLocks protocolResultsLocks;

	private final RequestsLocks filterIndexLocks;

	private final RequestsLocks protocolBatchMessagesLocks;

	private final Map<RequestIdentifier, Queue<BatchShareMessage>> batchMessagesReceived;

	private final Map<RequestIdentifier, Queue<CIntBatchShareMessage>> intBatchMessagesReceived;

	private final Map<RequestIdentifier, Queue<CLongBatchShareMessage>> longBatchMessagesReceived;


	// protocol results messages received
	private final Map<RequestIdentifier, Queue<ResultsMessage>> protocolResults;

	private final Map<RequestIdentifier, Queue<CIntBatchShareMessage>> intProtocolResults;

	private final Map<RequestIdentifier, Queue<CLongBatchShareMessage>> longProtocolResults;


	private final Map<RequestIdentifier, Queue<CIntBatchShareMessage>> filterIndex;


	private final CountDownLatch relayStarted;

	public SharemindMessageBroker() {
		protocolResultsLocks = new RequestsLocks();
		filterIndexLocks = new RequestsLocks();
		protocolBatchMessagesLocks = new RequestsLocks();

		relayStarted = new CountDownLatch(1);
		lock = new ReentrantLock();


		batchMessagesReceived = new ConcurrentHashMap<RequestIdentifier, Queue<BatchShareMessage>>();
		intBatchMessagesReceived = new ConcurrentHashMap<RequestIdentifier, Queue<CIntBatchShareMessage>>();
		longBatchMessagesReceived = new ConcurrentHashMap<RequestIdentifier, Queue<CLongBatchShareMessage>>();

		protocolResults = new ConcurrentHashMap<RequestIdentifier, Queue<ResultsMessage>>();
		intProtocolResults = new ConcurrentHashMap<RequestIdentifier, Queue<CIntBatchShareMessage>>();
		longProtocolResults = new ConcurrentHashMap<RequestIdentifier, Queue<CLongBatchShareMessage>>();

		filterIndex = new ConcurrentHashMap<RequestIdentifier, Queue<CIntBatchShareMessage>>();

	}

	public void receiveBatchMessage(BatchShareMessage message) {
		lock.lock();
		RequestIdentifier requestID = new RequestIdentifier(message
				.getRequestID().toByteArray(), message.getRegionID()
				.toByteArray());

		try {
			protocolBatchMessagesLocks.lockOnRequest(requestID);
			lock.unlock();

			if (batchMessagesReceived.containsKey(requestID)) {
				batchMessagesReceived.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<BatchShareMessage>();
				values.add(message);
				batchMessagesReceived.put(requestID, values);
			}

			protocolBatchMessagesLocks.signalToRead(requestID);

		} finally {
			protocolBatchMessagesLocks.unlockOnRequest(requestID);
		}
	}

    public void receiveBatchMessage(CIntBatchShareMessage message) {
        lock.lock();
        RequestIdentifier requestID = message.getRequestID();

        try {
            protocolBatchMessagesLocks.lockOnRequest(requestID);
            lock.unlock();

            if (intBatchMessagesReceived.containsKey(requestID)) {
                intBatchMessagesReceived.get(requestID).add(message);
            } else {
                Queue values = new ConcurrentLinkedQueue<CIntBatchShareMessage>();
                values.add(message);
                intBatchMessagesReceived.put(requestID, values);
            }

            protocolBatchMessagesLocks.signalToRead(requestID);

        } finally {
            protocolBatchMessagesLocks.unlockOnRequest(requestID);
        }
    }

	@Override
	public void receiveBatchMessage(CLongBatchShareMessage message) {
		lock.lock();
		RequestIdentifier requestID = message.getRequestID();

		try {
			protocolBatchMessagesLocks.lockOnRequest(requestID);
			lock.unlock();

			if (longBatchMessagesReceived.containsKey(requestID)) {
				longBatchMessagesReceived.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<CLongBatchShareMessage>();
				values.add(message);
				longBatchMessagesReceived.put(requestID, values);
			}

			protocolBatchMessagesLocks.signalToRead(requestID);

		} finally {
			protocolBatchMessagesLocks.unlockOnRequest(requestID);
		}
	}

	public Queue<BatchShareMessage> getReceivedBatchMessages(
            RequestIdentifier requestId) {
        lock.lock();

        try {

            protocolBatchMessagesLocks.lockOnRequest(requestId);
            lock.unlock();

            // While there aren't messages for this request wait.
            while (!batchMessagesReceived.containsKey(requestId)) {
                protocolBatchMessagesLocks.awaitForWrite(requestId);
            }

            return batchMessagesReceived.get(requestId);

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
    }



    public Queue<CIntBatchShareMessage> getReceivedBatchMessagesInt(
            RequestIdentifier requestId) {
        lock.lock();

        try {

            protocolBatchMessagesLocks.lockOnRequest(requestId);
            lock.unlock();

            // While there aren't messages for this request wait.
            while (!intBatchMessagesReceived.containsKey(requestId)) {
                protocolBatchMessagesLocks.awaitForWrite(requestId);
            }

            return intBatchMessagesReceived.get(requestId);

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

	@Override
	public Queue<CLongBatchShareMessage> getReceivedBatchMessagesLong(RequestIdentifier requestId) {
		lock.lock();
		try {
			protocolBatchMessagesLocks.lockOnRequest(requestId);
			lock.unlock();
			// While there aren't messages for this request wait.
			while (!longBatchMessagesReceived.containsKey(requestId)) {
				protocolBatchMessagesLocks.awaitForWrite(requestId);
			}
			return longBatchMessagesReceived.get(requestId);
		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}
	}


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
	public void receiveProtocolResults(CIntBatchShareMessage message) {
		lock.lock();
		RequestIdentifier requestID = message.getRequestID();

        try {
            protocolResultsLocks.lockOnRequest(requestID);
            lock.unlock();
            if (intProtocolResults.containsKey(requestID)) {
                intProtocolResults.get(requestID).add(message);
            } else {
				Queue values = new ConcurrentLinkedQueue<CIntBatchShareMessage>();
				values.add(message);
                intProtocolResults.put(requestID, values);
            }
            protocolResultsLocks.signalToRead(requestID);
        } finally {
            protocolResultsLocks.unlockOnRequest(requestID);
        }
    }

	@Override
	public void receiveProtocolResults(CLongBatchShareMessage message) {
		lock.lock();
		RequestIdentifier requestID = message.getRequestID();
		try {
			protocolResultsLocks.lockOnRequest(requestID);
			lock.unlock();
			if (longProtocolResults.containsKey(requestID)) {
				longProtocolResults.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<CLongBatchShareMessage>();
				values.add(message);
				longProtocolResults.put(requestID, values);
			}
			protocolResultsLocks.signalToRead(requestID);
		} finally {
			protocolResultsLocks.unlockOnRequest(requestID);
		}
	}

	public void receiveFilterIndex(CIntBatchShareMessage message) {

		lock.lock();
		RequestIdentifier requestID = message.getRequestID();

		try {
			filterIndexLocks.lockOnRequest(requestID);
			lock.unlock();
			if (filterIndex.containsKey(requestID)) {
				filterIndex.get(requestID).add(message);
			} else {
				Queue values = new ConcurrentLinkedQueue<CIntBatchShareMessage>();
				values.add(message);
				filterIndex.put(requestID, values);
			}
			filterIndexLocks.signalToRead(requestID);

		} finally {
			filterIndexLocks.unlockOnRequest(requestID);
		}

	}

	public Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID) {
		lock.lock();
		try {
			protocolResultsLocks.lockOnRequest(requestID);
			lock.unlock();
			/**
			 * Wait while protocol results do not arrive. Only two results
			 * should arrive, one from each of the remaining players.
			 */
			while (!(protocolResults.containsKey(requestID))
					|| protocolResults.get(requestID).size() < 2) {
				protocolResultsLocks.awaitForWrite(requestID);
			}

            return protocolResults.get(requestID);

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

	@Override
	public Queue<CIntBatchShareMessage> getIntProtocolResults(RequestIdentifier requestID) {
		lock.lock();
        try {
            protocolResultsLocks.lockOnRequest(requestID);
            lock.unlock();
            /**
             * Wait while protocol results do not arrive. Only two results
             * should arrive, one from each of the remaining players.
             */
            while (!(intProtocolResults.containsKey(requestID))
                    || intProtocolResults.get(requestID).size() < 2) {
                protocolResultsLocks.awaitForWrite(requestID);
            }

            return intProtocolResults.get(requestID);

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
	}

	@Override
	public Queue<CLongBatchShareMessage> getLongProtocolResults(RequestIdentifier requestID) {
		lock.lock();
		try {
			protocolResultsLocks.lockOnRequest(requestID);
			lock.unlock();
			/**
			 * Wait while protocol results do not arrive. Only two results
			 * should arrive, one from each of the remaining players.
			 */
			while (!(longProtocolResults.containsKey(requestID))
					|| longProtocolResults.get(requestID).size() < 2) {
				protocolResultsLocks.awaitForWrite(requestID);
			}

			return longProtocolResults.get(requestID);

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

	public CIntBatchShareMessage getFilterIndexes(RequestIdentifier requestID) {
		lock.lock();

		try {
			filterIndexLocks.lockOnRequest(requestID);
			lock.unlock();

			// While there aren't messages for this request wait.
			while (!filterIndex.containsKey(requestID)
					|| filterIndex.get(requestID).size() == 0) {
				filterIndexLocks.awaitForWrite(requestID);
			}

			return filterIndex.get(requestID).poll();

		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalArgumentException(ex.getMessage());
		}

	}

	public void relayStarted() {
		relayStarted.countDown();
	}

	public void waitRelayStart() throws InterruptedException {
		relayStarted.await();
	}

	public void allBatchMessagesRead(RequestIdentifier requestID) {
		protocolBatchMessagesLocks.removeLock(requestID);
		batchMessagesReceived.remove(requestID);
		intBatchMessagesReceived.remove(requestID);
	}

	public void readBatchMessages(RequestIdentifier requestID) {
		protocolBatchMessagesLocks.unlockOnRequest(requestID);
	}


	public void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException {
		protocolBatchMessagesLocks.awaitForWrite(requestID);

	}

	public void allResultsRead(RequestIdentifier requestID) {
		protocolResultsLocks.removeLock(requestID);
		protocolResults.remove(requestID);
		intProtocolResults.remove(requestID);
	}

	public void protocolResultsRead(RequestIdentifier requestID) {
		protocolResultsLocks.unlockOnRequest(requestID);
	}

	@Override
	public void intProtocolResultsRead(RequestIdentifier requestID) {
		protocolResultsRead(requestID);

	}

	@Override
	public void longProtocolResultsRead(RequestIdentifier requestID) {
		protocolResultsRead(requestID);
	}

	public void allIndexesMessagesRead(RequestIdentifier requestID) {
		filterIndexLocks.removeLock(requestID);
		filterIndex.remove(requestID);
	}

	public void indexMessageRead(RequestIdentifier requestID) {
		filterIndexLocks.unlockOnRequest(requestID);
	}

	public int numberofOfLocksResults() {
		return this.protocolResultsLocks.countLocks();
	}

	public boolean lockExistsForResultsOnRequest(RequestIdentifier requestID) {
		return this.protocolResultsLocks.lockExist(requestID);
	}

	/**
	 * Method only used for unitTest class implementations. Should be ignored on
	 * a concrete implementation
	 */
	public void receiveTestMessage(byte[] message) {
		throw new UnsupportedOperationException(
				"This method should only be used for testing purposes");

	}
}
