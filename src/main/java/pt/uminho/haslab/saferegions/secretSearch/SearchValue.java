package pt.uminho.haslab.saferegions.secretSearch;

import pt.uminho.haslab.saferegions.protocolresults.*;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;

public class SearchValue extends AbstractSearchValue {

	protected final List<byte[]> value;

	protected final int nBits;

	private final Map<BigInteger, Boolean> resultIndex;

	private final List<Boolean> resultsList;

	public SearchValue(int nBits, List<byte[]> value, Condition condition) {
		super(condition);
		this.value = value;
		this.nBits = nBits;
		resultIndex = new HashMap<BigInteger, Boolean>();
		resultsList = new ArrayList<Boolean>();
	}

	public boolean getRowClassification(byte[] row) {
		if (resultIndex.isEmpty()) {
			throw new IllegalStateException(
					"The method evaluateCondition must be evaluated before using this method");
		}
		return resultIndex.get(new BigInteger(row));
	}

	public void clearSearchIndexes() {
		resultIndex.clear();
	}

	public List<Boolean> getClassificationList() {
		return resultsList;
	}

	public void evaluateCondition(List<byte[]> cmpValues, List<byte[]> rowIDs,
			SharemindPlayer player) {

		List<Boolean> fIndex;
		try {
			List<byte[]> result;

			SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits);

			List<byte[]> values = new ArrayList<byte[]>();

			/* *
			 * Batch protocol comparison protocols require that the array of
			 * values being compared have the same size. As the value to be
			 * compared is always the same, it is created a list with the same
			 * size as the values being compared.
			 * 
			 * e.g: Values = [val1, val1, val1] cmpValues = [val2, val3, val4]
			 * In this example val1 is compared to every other value.
			 */

			if (value.size() == 1 && cmpValues.size() > 1) {
				// If there is only a single value replica-te it
				for (int i = 0; i < cmpValues.size(); i++) {
					values.add(value.get(0));
				}
			} else if (value.size() == cmpValues.size()) {
				values = value;
			} else {
				throw new IllegalStateException(
						"The size of values list being compared is invalid");
			}


			LOG.debug("Running protocol " + condition);
			if (condition == Equal) {
				result = ssf.equal(values, cmpValues, player);
			} else {
				result = ssf.greaterOrEqualThan(cmpValues, values, player);
			}

			if (player.isTargetPlayer()) {
				LOG.debug("is Target Player ");
				LOG.debug("Retrieve protocol results from peers");
				// At this point the size of the list identifiers must be 2.
				List<SearchResults> identifiers = player.getProtocolResults();

				identifiers.add(createBatchSearchResults(result, rowIDs));
				PlayerResults playerResults = new PlayerResults(identifiers,
						condition, nBits);
				fIndex = playerResults.declassify();
				/**
				 * if no matching element was found, an empty list is sent. When
				 * the other players receives an empty list, it knows that no
				 * index was found.
				 */
				List<byte[]> toSend = new ArrayList<byte[]>();

				for (int i = 0; i < fIndex.size(); i++) {
					Boolean b = fIndex.get(i);
					byte[] rowID = rowIDs.get(i);
					toSend.add(b.toString().getBytes());
					// Prepare the results to be send to the other players.
					resultIndex.put(new BigInteger(rowID), b);
					resultsList.add(b);

				}
				LOG.debug("Send filter results to peers");
				FilteredIndexes filtIndex = new FilteredIndexes(toSend);
				player.sendFilteredIndexes(filtIndex);

			} else {
				LOG.debug("Send protocol results to target");
				player.sendProtocolResults(createBatchSearchResults(result,
						rowIDs));
				List<byte[]> res = player.getFilterIndexes().getIndexes();

				for (int i = 0; i < res.size(); i++) {
					byte[] val = res.get(i);
					byte[] rowID = rowIDs.get(i);
					Boolean decRes = Boolean.parseBoolean(new String(val));
					resultIndex.put(new BigInteger(rowID), decRes);
					resultsList.add(decRes);
				}
			}
		} catch (InvalidSecretValue | ResultsLengthMismatch | InvalidNumberOfBits | ResultsIdentifiersMismatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

}
