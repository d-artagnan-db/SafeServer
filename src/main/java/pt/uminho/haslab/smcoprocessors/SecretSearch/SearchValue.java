package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.PlayerResults;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecretFunctions;

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
		LOG.debug("Going to evaluate function evaluateCondition");
		List<Boolean> fIndex;
		try {
			List<byte[]> result;

			LOG.debug("Going to run protocol " + condition);
			BigInteger bMod = BigInteger.valueOf(2).pow(nBits);
			SharemindSecretFunctions ssf = new SharemindSecretFunctions(nBits,
					bMod);

			List<byte[]> values = new ArrayList<byte[]>();

			// System.out.println("EvaluateConfition "+ value.size());
			// System.out.println("EvaluateCondition "+ cmpValues.size());
			/**
			 * Batch protocol comparison protocols require that the array of
			 * values being compared have the same size. As the value to be
			 * compared is always the same, it is created a list with the same
			 * size as the values being compared.
			 * 
			 * e.g: Values = [val1, val1, val1] cmpValues = [val2, val3, val4]
			 * In this example val1 is compared to every other value.
			 */
			if (value.size() == 1) {
				// If there is only a single value replica-te it
				for (int i = 0; i < cmpValues.size(); i++) {
					values.add(value.get(0));
				}
			} else if (value.size() == cmpValues.size()) {
				values = value;
			} else {
				LOG.debug("The size of values list being compared is invalid");
				throw new IllegalStateException(
						"The size of values list being compared is invalid");
			}

			// System.out.println("Values size "+ values.size());
			if (condition == Equal) {
				result = ssf.equal(values, cmpValues, player);
			} else {
				result = ssf.greaterOrEqualThan(cmpValues, values, player);
			}
			// System.out.println("Values size "+ values.size());
			// System.out.println("Cmp value size "+ cmpValues.size());
			// System.out.println("Results size "+ result.size());
			LOG.debug("Protocol completed");
			LOG.debug("Is targetPlayer " + player.isTargetPlayer());
			if (player.isTargetPlayer()) {
				LOG.debug("Going to search for a matching index");

				// At this point the size of the list identifiers must be 2.
				List<SearchResults> identifiers = player.getProtocolResults();
				// System.out.println("Identifiers size "+identifiers.size());
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
			} else {
				player.sendProtocolResults(targetPlayer,
						createBatchSearchResults(result, rowID));
				List<byte[]> res = player.getFilterIndexes().getIndexes();
				fIndex = new ArrayList<Boolean>();

				for (byte[] val : res) {
					fIndex.add(Boolean.parseBoolean(new String(val)));
				}

			}

			LOG.debug("Going to clean results ");

			player.cleanResultsMatch();

		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsLengthMissmatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsIdentifiersMissmatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

		LOG.debug("Function evaluateCondition returns " + fIndex.size());

		return fIndex;
	}

}
