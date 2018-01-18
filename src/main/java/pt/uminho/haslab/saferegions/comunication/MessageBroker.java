package pt.uminho.haslab.saferegions.comunication;

import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.*;

import java.util.Queue;

public interface MessageBroker {

	void receiveProtocolResults(ResultsMessage message);

    void receiveProtocolResults(IntResultsMessage message);

    void receiveProtocolResults(LongResultsMessage message);

    void receiveFilterIndex(FilterIndexMessage message);

	void relayStarted();

	void waitRelayStart() throws InterruptedException;

	Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

	Queue<IntResultsMessage> getIntProtocolResults(RequestIdentifier requestID);

    Queue<Search.LongResultsMessage> getLongProtocolResults(RequestIdentifier requestID);

    FilterIndexMessage getFilterIndexes(RequestIdentifier requestID);

	void allResultsRead(RequestIdentifier requestID);

	void protocolResultsRead(RequestIdentifier requestID);

    void intProtocolResultsRead(RequestIdentifier requestID);

    void longProtocolResultsRead(RequestIdentifier requestID);


    void allIndexesMessagesRead(RequestIdentifier requestID);

	void indexMessageRead(RequestIdentifier requestID);

	void receiveBatchMessage(CIntBatchShareMessage message);

    void receiveBatchMessage(CLongBatchShareMessage message);

    void receiveBatchMessage(BatchShareMessage message);

	void receiveTestMessage(byte[] message);

	Queue<BatchShareMessage> getReceivedBatchMessages(
			RequestIdentifier requestId);

    Queue<CIntBatchShareMessage> getReceivedBatchMessagesInt(
            RequestIdentifier requestId);

    Queue<CLongBatchShareMessage> getReceivedBatchMessagesLong(
            RequestIdentifier requestId);

	void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException;

	void allBatchMessagesRead(RequestIdentifier requestID);

	void readBatchMessages(RequestIdentifier requestID);

}
