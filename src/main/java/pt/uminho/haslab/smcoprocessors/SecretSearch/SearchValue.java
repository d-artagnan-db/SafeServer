package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.protocolresults.DataIdentifiers;
import pt.uminho.haslab.smcoprocessors.protocolresults.FilteredIndexes;
import pt.uminho.haslab.smcoprocessors.protocolresults.PlayerResults;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;

public class SearchValue extends AbstractSearchValue {

	static final Log LOG = LogFactory.getLog(SearchValue.class.getName());

	protected final byte[] value;

	protected final int nBits;

	public SearchValue(int nBits, byte[] value, Condition condition,
			int targetPlayer) {
		super(condition, targetPlayer);
		this.value = value;
		this.nBits = nBits;
	}

	public Condition getCondition() {
		return condition;
	}

	public boolean evaluateCondition(byte[] value, byte[] rowID,
			SharemindPlayer player) {
		LOG.debug("Going to evaluate function evaluateCondition");
		FilteredIndexes filtIndex;

		try {
			Secret result;

			BigInteger bOrigValue = new BigInteger(this.value);
			BigInteger bCmpValue = new BigInteger(value);

			Secret originalSecret = generateSecret(nBits, bOrigValue, player);
			Secret cmpSecret = generateSecret(nBits, bCmpValue, player);

			LOG.debug("Going to run protocol " + condition);

			if (condition == Equal) {
				result = originalSecret.equal(cmpSecret);
			} else {
				result = cmpSecret.greaterOrEqualThan(originalSecret);
			}

			LOG.debug("Protocol completed");
			LOG.debug("Is targetPlayer " + player.isTargetPlayer());
			if (player.isTargetPlayer()) {
				LOG.debug("Going to search for a matching index");

				// At this point the size of the list identifiers must be 2.
				List<DataIdentifiers> identifiers = player.getProtocolResults();
				identifiers.add(createSearchResults(result, rowID)
						.toDataIdentifier());
				PlayerResults playerResults = new PlayerResults(identifiers,
						condition);
				BigInteger index = playerResults.findCorrespondingIndex();
				List<byte[]> indexes = new ArrayList<byte[]>();
				// In here an exception should be thrown
				if (index != null) {
					indexes.add(index.toByteArray());
				}
				/**
				 * if no matching element was found, index is null and an empty
				 * list is sent. When the other players receives an empty list,
				 * it knows that no index was found.
				 */
				filtIndex = new FilteredIndexes(indexes);
				player.sendFilteredIndexes(filtIndex);
			} else {
				player.sendProtocolResults(targetPlayer,
						createSearchResults(result, rowID));
				filtIndex = player.getFilterIndexes();

			}

			LOG.debug("Going to clean results ");

			player.cleanResultsMatch();
			if (!filtIndex.isEmpty()) {
				LOG.debug("Protocol result is true");
				return true;
			}

		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsLengthMissmatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (ResultsIdentifiersMissmatch ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}

		LOG.debug("Function evaluateCondition returns default false");

		return false;
	}

}
