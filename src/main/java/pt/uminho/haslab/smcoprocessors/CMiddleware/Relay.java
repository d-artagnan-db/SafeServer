package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.io.IOException;
import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

public interface Relay {


	public void sendBatchMessages(BatchShareMessage msg) throws IOException;

	public void sendProtocolResults(ResultsMessage msg) throws IOException;

	public void stopRelay() throws IOException;

	public boolean isRelayRunning();

	public void bootRelay();

	public void forceStopRelay() throws IOException;

	public void sendFilteredIndexes(Search.FilterIndexMessage msg)
			throws IOException;

}
