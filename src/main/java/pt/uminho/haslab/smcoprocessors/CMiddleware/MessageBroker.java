package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.Queue;
import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;

public interface MessageBroker {

	public void receiveMessage(ShareMessage message);

	public void receiveProtocolResults(ResultsMessage message);

	public void receiveFilterIndex(FilterIndexMessage message);

	public void relayStarted();

	public void waitRelayStart() throws InterruptedException;

	public Queue<ShareMessage> getReceivedMessages(RequestIdentifier requestID);

	public Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

	public FilterIndexMessage getFilterIndexes(RequestIdentifier requestID);

	public void waitNewMessage(RequestIdentifier requestID)
			throws InterruptedException;

	public void allMessagesRead(RequestIdentifier requestID);

	public void readMessages(RequestIdentifier requestID);

	public void allResultsRead(RequestIdentifier requestID);

	public void protocolResultsRead(RequestIdentifier requestID);

	public void allIndexesMessagesRead(RequestIdentifier requestID);

	public void indexeMessageRead(RequestIdentifier requestID);

}
