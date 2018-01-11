package pt.uminho.haslab.saferegions.comunication;

import pt.uminho.haslab.protocommunication.Search.FilterIndexMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.IntResultsMessage;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;



import java.io.IOException;

public interface Relay {

	void sendBatchMessages(BatchShareMessage msg) throws IOException;

	void sendBatchMessages(CIntBatchShareMessage msg) throws IOException;

	void sendProtocolResults(ResultsMessage msg) throws IOException;

	void sendProtocolResults(IntResultsMessage msg) throws IOException;

	void stopRelay() throws IOException;

	boolean isRelayRunning();

	void bootRelay();

	void forceStopRelay() throws IOException;

	void sendFilteredIndexes(FilterIndexMessage msg) throws IOException;

	void registerRequest(RequestIdentifier requestIdentifier);

	void unregisterRequest(RequestIdentifier requestIdentifier);

}
