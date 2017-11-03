package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.smcoprocessors.protocolresults.*;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Equal;

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
		System.out.println("Get Row Classification "+ row);
		if (resultIndex.isEmpty()) {
			throw new IllegalStateException(
					"The method evaluateCondition must be evaluated before using this method");
		}
		System.out.println("Going to get classification" + new String(row) + " -> " + resultIndex.get(row));

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

			LOG.debug("Player with id " + player.getPlayerID()
					+ " is going to run protocol " + condition);
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
			LOG.debug("Player with id " + player.getPlayerID() + " has "
					+ value.size() + " values and " + cmpValues.size()
					+ " cmpValues");

			if (value.size() == 1 && cmpValues.size() > 1) {
				LOG.debug("Player with id " + player.getPlayerID()
						+ " is replicating value " +  new BigInteger(value.get(0)));
				// If there is only a single value replica-te it
				for (int i = 0; i < cmpValues.size(); i++) {
					values.add(value.get(0));
				}
			} else if (value.size() == cmpValues.size()) {
				values = value;
			} else {
				LOG.error("The size of values list being compared is invalid");
				throw new IllegalStateException(
						"The size of values list being compared is invalid");
			}

			for(int i=0; i < cmpValues.size(); i++){
				BigInteger val = new BigInteger(cmpValues.get(i));
				String rowID = new String(rowIDs.get(i));
				LOG.debug("Player " + player.getPlayerID() + " has input batch value of " + rowID +" -> " + val);
			}

			if (condition == Equal) {
				LOG.debug("Going to run equals");
				result = ssf.equal(values, cmpValues, player);
			} else {
				LOG.debug("Going to run greaterOrEqualThan");
				result = ssf.greaterOrEqualThan(cmpValues, values, player);
			}
			LOG.debug("Player with id " + player.getPlayerID()
					+ " completed protocol");

			if (player.isTargetPlayer()) {
				LOG.debug("Player with id " + +player.getPlayerID()
						+ " is searching for  matching index");

				// At this point the size of the list identifiers must be 2.
				List<SearchResults> identifiers = player.getProtocolResults();
				LOG.debug("Player with id " + +player.getPlayerID()
						+ " is declassifying results");

				identifiers.add(createBatchSearchResults(result, rowIDs));
				PlayerResults playerResults = new PlayerResults(identifiers,
						condition, nBits);
				fIndex = playerResults.declassify();
				LOG.debug("Declassify result size is " + fIndex.size());
				/**
				 * if no matching element was found, an empty list is sent. When
				 * the other players receives an empty list, it knows that no
				 * index was found.
				 */
				List<byte[]> toSend = new ArrayList<byte[]>();

				for (int i = 0; i < fIndex.size(); i++) {
					// Set resulting index of rows that pass or fail the
					// condition
					Boolean b = fIndex.get(i);
					byte[] rowID = rowIDs.get(i);
					toSend.add(b.toString().getBytes());
					LOG.debug("Going to store result val " + new String(rowID) + " -> " + b);
					// Prepare the results to be send to the other players.
					System.out.println("Going to store result val " + new String(rowID) + " -> " + b);
					resultIndex.put(new BigInteger(rowID), b);
					resultsList.add(b);

				}
				LOG.debug("FiltIndex size is  "+ toSend.size());
				FilteredIndexes filtIndex = new FilteredIndexes(toSend);
				player.sendFilteredIndexes(filtIndex);
				LOG.debug("Player with id " + +player.getPlayerID()
						+ " sent declassified results");

			} else {
				LOG.debug("Player with id " + player.getPlayerID()
						+ " will send protocol results");
				player.sendProtocolResults(createBatchSearchResults(result,
						rowIDs));
				LOG.debug("Player with id " + player.getPlayerID()
						+ " is waiting for filterIndexes");
				List<byte[]> res = player.getFilterIndexes().getIndexes();

				for (int i = 0; i < res.size(); i++) {
					byte[] val = res.get(i);
					byte[] rowID = rowIDs.get(i);
					Boolean decRes = Boolean.parseBoolean(new String(val));
					System.out.println("Going to store result val " + new String(rowID) + " -> " + decRes);

					resultIndex.put(new BigInteger(rowID), decRes);
					resultsList.add(decRes);
				}
			}
		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsLengthMismatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsIdentifiersMismatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

}
