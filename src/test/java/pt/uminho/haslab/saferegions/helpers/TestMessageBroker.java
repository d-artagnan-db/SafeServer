package pt.uminho.haslab.saferegions.helpers;

import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 * Mock class used for testing purpose that only leaves the a test method to be
 * defined.
 */
public abstract class TestMessageBroker implements MessageBroker {
	private final CountDownLatch relayStarted;

	public TestMessageBroker() {
		this.relayStarted = new CountDownLatch(1);
	}

	public void receiveProtocolResults(Search.ResultsMessage message) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void receiveFilterIndex(Search.FilterIndexMessage message) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void relayStarted() {
		relayStarted.countDown();
	}

	public void waitRelayStart() throws InterruptedException {
		relayStarted.await();
	}

	public Queue<Search.ResultsMessage> getProtocolResults(
			RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public Search.FilterIndexMessage getFilterIndexes(
			RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void waitNewMessage(RequestIdentifier requestID)
			throws InterruptedException {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");

	}

	public void readMessages(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");

	}

	public void allResultsRead(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");

	}

	public void protocolResultsRead(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void allIndexesMessagesRead(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void indexMessageRead(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void receiveBatchMessage(Search.BatchShareMessage message) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public Queue<Search.BatchShareMessage> getReceivedBatchMessages(
			RequestIdentifier requestId) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void waitNewBatchMessage(RequestIdentifier requestID)
			throws InterruptedException {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void allBatchMessagesRead(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}

	public void readBatchMessages(RequestIdentifier requestID) {
		throw new UnsupportedOperationException(
				"Method not currently supported on testing framework");
	}
}
