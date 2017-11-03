package pt.uminho.haslab.smcoprocessors.secretSearch;

import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.smhbase.interfaces.Player;

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
