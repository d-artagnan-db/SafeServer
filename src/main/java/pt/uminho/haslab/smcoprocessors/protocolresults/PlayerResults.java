package pt.uminho.haslab.smcoprocessors.protocolresults;

import java.math.BigInteger;
import java.util.List;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.GreaterOrEqualThan;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

public class PlayerResults {

	private final List<DataIdentifiers> results;
	private final Condition condition;

	/* TODO: this function has to validate if the protocol results are all ok. */
	public PlayerResults(List<DataIdentifiers> results, Condition condition)
			throws ResultsLengthMissmatch {
		int nIdentifiers = results.get(0).getIdentifiers().size();

		for (DataIdentifiers d : results) {
			if (nIdentifiers != d.getIdentifiers().size()) {
				throw new ResultsLengthMissmatch();
			}
		}
		this.condition = condition;
		this.results = results;
	}

	/**
	 * Function that given the results from each player it returns the index of
	 * the row that correspond to the real key searched for.
	 * 
	 * Currently this is being used with just a single element in each list,
	 * thus making it possible to know if a value matches or not. Previous
	 * implementations used lists with more elements, but this was left mostly
	 * unchanged to not break compatability. In the future this code can
	 * refactoring and simplified.
	 * 
	 * 
	 * @return BigInteger with the corresponding Index
	 * @throws pt.uminho.haslab.smcoprocessors.protocolresults.ResultsIdentifiersMissmatch
	 */
	public BigInteger findCorrespondingIndex()
			throws ResultsIdentifiersMissmatch {
		int nIdentifiers = results.get(0).getIdentifiers().size();

		// BigInteger mIndex = null;
		for (int i = 0; i < nIdentifiers; i++) {

			BigInteger firstIdent = results.get(0).getIdentifiers().get(i);
			BigInteger secondIdent = results.get(1).getIdentifiers().get(i);
			BigInteger thirdIdent = results.get(2).getIdentifiers().get(i);

			BigInteger firstSecret = results.get(0).getSecrets().get(i);
			BigInteger secondSecret = results.get(1).getSecrets().get(i);
			BigInteger thirdSecret = results.get(2).getSecrets().get(i);

			// Check if identifiers match
			if (!firstIdent.equals(secondIdent)
					|| !secondIdent.equals(thirdIdent)) {
				throw new ResultsIdentifiersMissmatch();
			}

			// String currentID = new String(results.get(0).getIdentifiers()
			// .get(i).toByteArray());
			// System.out.println(currentID+" firstSecret "+ firstSecret);
			// System.out.println(currentID+" secondSecret "+ secondSecret);
			// System.out.println(currentID+" thirdSecret "+ thirdSecret);

			SharemindSharedSecret secretResult = new SharemindSharedSecret(1,
					firstSecret, secondSecret, thirdSecret);
			// System.out.println(i+" searching "+currentID+" seeking a match "+
			// secretResult.unshare().intValue());
			int result = secretResult.unshare().intValue();
			if (condition == Equal && result == 1) {
				return results.get(0).getIdentifiers().get(i);
			} else if (condition == GreaterOrEqualThan && result == 0) {
				return results.get(0).getIdentifiers().get(i);
			}
		}

		// If match is not found, null is returned
		return null;
	}
}
