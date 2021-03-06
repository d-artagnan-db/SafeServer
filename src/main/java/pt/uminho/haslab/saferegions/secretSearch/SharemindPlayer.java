package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.util.List;

/**
 * @author roger
 */
public interface SharemindPlayer extends Player {

    void sendProtocolResults(List<byte[]> dest);

    void sendIntProtocolResults(int[] dest);

    void sendLongProtocolResults(long[] dest);

    /**
     * This function accepts the local results, retrieves the results from the
     * message broker and joins all the results to obtain the final correct
     * result that contains the real HBase keys.
     *
     * @return
     * @throws ResultsLengthMismatch
     */
    List<List<byte[]>> getProtocolResults() throws ResultsLengthMismatch;

    List<int[]> getIntProtocolResults() throws ResultsLengthMismatch;

    List<long[]> getLongProtocolResults() throws ResultsLengthMismatch;

    void cleanValues();

    void cleanResultsMatch();

    void sendFilteredIndexes(int[] results);

    int[] getFilterIndexes();

    boolean isTargetPlayer();

    // To define dest player use the function setTargetPlayer
    void setTargetPlayer(int targetPlayerID);


}
