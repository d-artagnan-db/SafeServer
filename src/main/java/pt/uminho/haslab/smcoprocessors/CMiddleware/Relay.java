package pt.uminho.haslab.smcoprocessors.CMiddleware;

import pt.uminho.haslab.protocommunication.Search;
import pt.uminho.haslab.protocommunication.Search.BatchShareMessage;
import pt.uminho.haslab.protocommunication.Search.ResultsMessage;

import java.io.IOException;

public interface Relay {

    void sendBatchMessages(BatchShareMessage msg) throws IOException;

    void sendProtocolResults(ResultsMessage msg) throws IOException;

    void stopRelay() throws IOException;

    boolean isRelayRunning();

    void bootRelay();

    void forceStopRelay() throws IOException;

    void sendFilteredIndexes(Search.FilterIndexMessage msg) throws IOException;

}
