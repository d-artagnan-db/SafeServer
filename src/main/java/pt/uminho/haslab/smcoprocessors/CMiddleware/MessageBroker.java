package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.Queue;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

public interface MessageBroker {

	public void receiveProtocolResults(ResultsMessage message);

	public void receiveFilterIndex(FilterIndexMessage message);

	public void relayStarted();

	public void waitRelayStart() throws InterruptedException;

	public Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

	public FilterIndexMessage getFilterIndexes(RequestIdentifier requestID);

	public void waitNewMessage(RequestIdentifier requestID)
			throws InterruptedException;

	public void readMessages(RequestIdentifier requestID);

	public void allResultsRead(RequestIdentifier requestID);

	public void protocolResultsRead(RequestIdentifier requestID);

	public void allIndexesMessagesRead(RequestIdentifier requestID);

	public void indexeMessageRead(RequestIdentifier requestID);

	public void receiveBatchMessage(BatchShareMessage message);

	public Queue<BatchShareMessage> getReceivedBatchMessages(
			RequestIdentifier requestId);

	public void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException;

	public void allBatchMessagesRead(RequestIdentifier requestID);

	public void readBatchMessages(RequestIdentifier requestID);
}
