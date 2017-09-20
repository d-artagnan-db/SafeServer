package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.protocolresults.*;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Equal;

public class SearchValue extends AbstractSearchValue {

	static final Log LOG = LogFactory.getLog(SearchValue.class.getName());

	protected final List<byte[]> value;

	protected final int nBits;

	public SearchValue(int nBits, List<byte[]> value, Condition condition,
			int targetPlayer) {
		super(condition, targetPlayer);
		this.value = value;
		this.nBits = nBits;
	}

	public Condition getCondition() {
		return condition;
	}

	public List<Boolean> evaluateCondition(List<byte[]> cmpValues,
			List<byte[]> rowID, SharemindPlayer player) {

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
						+ " is replicating values");
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

			if (condition == Equal) {
				// LOG.debug("Going to run equals");
				result = ssf.equal(values, cmpValues, player);
			} else {
				// LOG.debug("Going to run greaterOrEqualThan");
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

				identifiers.add(createBatchSearchResults(result, rowID));
				PlayerResults playerResults = new PlayerResults(identifiers,
						condition, nBits);
				fIndex = playerResults.declassify();

				/**
				 * if no matching element was found, an empty list is sent. When
				 * the other players receives an empty list, it knows that no
				 * index was found.
				 */
				List<byte[]> toSend = new ArrayList<byte[]>();
				for (Boolean b : fIndex) {
					toSend.add(b.toString().getBytes());
				}

				FilteredIndexes filtIndex = new FilteredIndexes(toSend);
				player.sendFilteredIndexes(filtIndex);
				LOG.debug("Player with id " + +player.getPlayerID()
						+ " sent declassified results");
			} else {
				LOG.debug("Player with id " + player.getPlayerID()
						+ " will send protocol results");
				player.sendProtocolResults(targetPlayer,
						createBatchSearchResults(result, rowID));
				LOG.debug("Player with id " + player.getPlayerID()
						+ " is waiting for filterIndexes");
				List<byte[]> res = player.getFilterIndexes().getIndexes();
				fIndex = new ArrayList<Boolean>();

				for (byte[] val : res) {
					Boolean decRes = Boolean.parseBoolean(new String(val));
					fIndex.add(decRes);
				}
			}
			// player.cleanResultsMatch();

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

		return fIndex;
	}

}
