package pt.uminho.haslab.saferegions.comunication;

import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

import java.util.Queue;

public interface MessageBroker {

	void receiveProtocolResults(ResultsMessage message);

    void receiveProtocolResults(CIntBatchShareMessage message);

    void receiveProtocolResults(CLongBatchShareMessage message);

    void receiveFilterIndex(CIntBatchShareMessage message);

	void relayStarted();

	void waitRelayStart() throws InterruptedException;

	Queue<ResultsMessage> getProtocolResults(RequestIdentifier requestID);

    Queue<CIntBatchShareMessage> getIntProtocolResults(RequestIdentifier requestID);

    Queue<CLongBatchShareMessage> getLongProtocolResults(RequestIdentifier requestID);

    CIntBatchShareMessage getFilterIndexes(RequestIdentifier requestID);

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
