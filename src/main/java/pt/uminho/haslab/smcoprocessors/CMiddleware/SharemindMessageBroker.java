package pt.uminho.haslab.smcoprocessors.CMiddleware;

import com.sun.xml.internal.ws.server.UnsupportedMediaException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
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

    // Locks for protocol messages exchange
    private final RequestsLocks protocolMessagesLocks;

    // Locks for protocol results
    private final RequestsLocks protocolResultsLocks;

    private final RequestsLocks filterIndexLocks;

    private final RequestsLocks protocolBatchMessagesLocks;

    private final Map<RequestIdentifier, Queue<BatchShareMessage>> batchMessagesReceived;

    // protocol resuts messages received
    private final Map<RequestIdentifier, Queue<ResultsMessage>> protocolResults;

    private final Map<RequestIdentifier, FilterIndexMessage> filterIndex;

    private final CountDownLatch relayStarted;

    public SharemindMessageBroker() {
        protocolMessagesLocks = new RequestsLocks();
        protocolResultsLocks = new RequestsLocks();
        filterIndexLocks = new RequestsLocks();

        protocolBatchMessagesLocks = new RequestsLocks();

        relayStarted = new CountDownLatch(1);
        lock = new ReentrantLock();

        protocolResults = new ConcurrentHashMap<RequestIdentifier, Queue<ResultsMessage>>();
        filterIndex = new ConcurrentHashMap<RequestIdentifier, FilterIndexMessage>();

        batchMessagesReceived = new ConcurrentHashMap<RequestIdentifier, Queue<BatchShareMessage>>();

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

    public Queue<BatchShareMessage> getReceivedBatchMessages(
            RequestIdentifier requestId) {
        lock.lock();

        try {

            protocolBatchMessagesLocks.lockOnRequest(requestId);
            lock.unlock();

            // While there arent messages for this request wait.
            while (!batchMessagesReceived.containsKey(requestId)) {
                LOG.debug("Waiting on messages");
                protocolBatchMessagesLocks.awaitForWrite(requestId);
            }

            return batchMessagesReceived.get(requestId);

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
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

            while (!(protocolResults.containsKey(requestID) && protocolResults
                    .get(requestID).size() == 2)) {
                protocolResultsLocks.awaitForWrite(requestID);
            }
            return protocolResults.get(requestID);

        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

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
    }

    public void readMessages(RequestIdentifier requestID) {
        protocolMessagesLocks.unlockOnRequest(requestID);
    }

    public void readBatchMessages(RequestIdentifier requestID) {
        protocolBatchMessagesLocks.unlockOnRequest(requestID);
    }

    public void waitNewMessage(RequestIdentifier requestID)
            throws InterruptedException {
        protocolMessagesLocks.awaitForWrite(requestID);
    }

    public void waitNewBatchMessage(RequestIdentifier requestID)
            throws InterruptedException {
        protocolBatchMessagesLocks.awaitForWrite(requestID);

    }

    public void allResultsRead(RequestIdentifier requestID) {
        protocolResultsLocks.removeLock(requestID);
        protocolResults.remove(requestID);
    }

    public void protocolResultsRead(RequestIdentifier requestID) {
        protocolResultsLocks.unlockOnRequest(requestID);
    }

    public void allIndexesMessagesRead(RequestIdentifier requestID) {
        filterIndex.remove(requestID);
        filterIndexLocks.removeLock(requestID);
    }

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

    /**
     * Method only used for unitTest class implementations.
     * Should be ignored on a concrete implementation
     */
    public void receiveTestMessage(byte[] message) {
        throw new UnsupportedMediaException("This method should only be used for testing purposes");

    }
}
