package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.protocolresults.FilteredIndexes;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.saferegions.protocolresults.SearchResults;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.util.List;

/**
 * @author roger
 */
public interface SharemindPlayer extends Player {

	// To define dest player use the function setTargetPlayer
	void sendProtocolResults(SearchResults res);

	/**
	 * This function accepts the local results, retrieves the results from the
	 * message broker and joins all the results to obtain the final correct
	 * result that contains the real HBase keys.
	 * 
	 * @return
	 * @throws ResultsLengthMismatch
	 */
	List<SearchResults> getProtocolResults() throws ResultsLengthMismatch;

	void cleanValues();

	void cleanResultsMatch();

	void sendFilteredIndexes(FilteredIndexes indexes);

	FilteredIndexes getFilterIndexes();

	boolean isTargetPlayer();

	void setTargetPlayer(int targetPlayerID);

}
