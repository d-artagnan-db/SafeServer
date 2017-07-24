package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.Queue;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

public interface MessageBroker {

	void receiveProtocolResults(ResultsMessage message);

	void receiveFilterIndex(FilterIndexMessage message);

	void relayStarted();

	void waitRelayStart() throws InterruptedException;

	Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

	FilterIndexMessage getFilterIndexes(RequestIdentifier requestID);

	void waitNewMessage(RequestIdentifier requestID)
			throws InterruptedException;

	void readMessages(RequestIdentifier requestID);

	void allResultsRead(RequestIdentifier requestID);

	void protocolResultsRead(RequestIdentifier requestID);

	void allIndexesMessagesRead(RequestIdentifier requestID);

	void indexeMessageRead(RequestIdentifier requestID);

	void receiveBatchMessage(BatchShareMessage message);

	Queue<BatchShareMessage> getReceivedBatchMessages(
            RequestIdentifier requestId);

	void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException;

	void allBatchMessagesRead(RequestIdentifier requestID);

	void readBatchMessages(RequestIdentifier requestID);
}
