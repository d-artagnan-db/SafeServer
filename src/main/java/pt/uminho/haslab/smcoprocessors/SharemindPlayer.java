package pt.uminho.haslab.smcoprocessors;

import java.util.List;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.smhbase.interfaces.Player;

/**
 * 
 * @author roger
 */
public interface SharemindPlayer extends Player {

	void sendProtocolResults(int destPlayer, SearchResults res);

	/**
	 * This function accepts the local results, retrieves the results from the
	 * message broker and joins all the results to obtain the final correct
	 * result that contains the real HBase keys.
	 * 
	 * @return
	 * @throws pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch
	 */
    List<SearchResults> getProtocolResults()
			throws ResultsLengthMissmatch;

	void cleanValues();

	void cleanResultsMatch();

	void sendFilteredIndexes(FilteredIndexes indexes);

	FilteredIndexes getFilterIndexes();

	boolean isTargetPlayer();

	void setTargetPlayer();

}
