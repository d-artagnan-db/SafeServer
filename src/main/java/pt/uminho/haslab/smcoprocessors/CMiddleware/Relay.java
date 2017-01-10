package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.io.IOException;
import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;
import pt.uminho.haslab.protocommunication.Search.ShareMessage;

public interface Relay {

	public void sendMessage(ShareMessage msg) throws IOException;

	public void sendProtocolResults(ResultsMessage msg) throws IOException;

	public void stopRelay() throws IOException;

	public boolean isRelayRunning();

	public void bootRelay();

	public void forceStopRelay() throws IOException;

	public void sendFilteredIndexes(Search.FilterIndexMessage msg)
			throws IOException;

}
