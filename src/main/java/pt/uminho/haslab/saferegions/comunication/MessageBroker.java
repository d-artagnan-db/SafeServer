package pt.uminho.haslab.saferegions.comunication;

import pt.uminho.haslab.protocommunication.Search.IntBatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.IntResultsMessage;

import java.util.Queue;

public interface MessageBroker {

	void receiveProtocolResults(ResultsMessage message);

    void receiveProtocolResults(IntResultsMessage message);

	void receiveFilterIndex(FilterIndexMessage message);

	void relayStarted();

	void waitRelayStart() throws InterruptedException;

	Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

	Queue<IntResultsMessage> getIntProtocolResults(RequestIdentifier requestID);

	FilterIndexMessage getFilterIndexes(RequestIdentifier requestID);

	void allResultsRead(RequestIdentifier requestID);

	void protocolResultsRead(RequestIdentifier requestID);

    void intProtocolResultsRead(RequestIdentifier requestID);

	void allIndexesMessagesRead(RequestIdentifier requestID);

	void indexMessageRead(RequestIdentifier requestID);

	void receiveBatchMessage(IntBatchShareMessage message);

	void receiveBatchMessage(BatchShareMessage message);

	void receiveTestMessage(byte[] message);

	Queue<BatchShareMessage> getReceivedBatchMessages(
			RequestIdentifier requestId);

    Queue<IntBatchShareMessage> getReceivedBatchMessagesInt(
            RequestIdentifier requestId);

	void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException;

	void allBatchMessagesRead(RequestIdentifier requestID);

	void readBatchMessages(RequestIdentifier requestID);
}
