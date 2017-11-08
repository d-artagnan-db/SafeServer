package pt.uminho.haslab.smcoprocessors.protocolresults;

import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;

public class PlayerResults {

	private final List<SearchResults> results;
	private final Condition condition;
	private final int nBits;

	/* TODO: this function has to validate if the protocol results are all ok. */
	public PlayerResults(List<SearchResults> results, Condition condition,
			int nBits) throws ResultsLengthMismatch {

		/*
		 * for(int i =0; i < results.size(); i++){ results.get(i).printSize(); }
		 */

		int nIdentifiers = results.get(0).getIdentifiers().size();

		for (SearchResults d : results) {
			if (nIdentifiers != d.getIdentifiers().size()) {
				throw new ResultsLengthMismatch();
			}
		}
		this.condition = condition;
		this.results = results;
		this.nBits = nBits;
	}

	/**
	 * Iterates through the results of the smpc protocols and declassifies the
	 * result. The function returns True for row keys that satisfy the
	 * protocols.
	 * 
	 * @return BigInteger with the corresponding Index
	 * @throws ResultsIdentifiersMismatch
	 */
	public List<Boolean> declassify() throws ResultsIdentifiersMismatch {
		int nIdentifiers = results.get(0).getIdentifiers().size();

		List<Boolean> resultIDS = new ArrayList<Boolean>();

		for (int i = 0; i < nIdentifiers; i++) {
			byte[] bFirstIdent = results.get(0).getIdentifiers().get(i);
			byte[] bSecondIdent = results.get(1).getIdentifiers().get(i);
			byte[] bThirdIdent = results.get(2).getIdentifiers().get(i);

			byte[] bFirstSecret = results.get(0).getSecrets().get(i);
			byte[] bSecondSecret = results.get(1).getSecrets().get(i);
			byte[] bThirdSecret = results.get(2).getSecrets().get(i);

			BigInteger firstIdent = new BigInteger(bFirstIdent);
			BigInteger secondIdent = new BigInteger(bSecondIdent);
			BigInteger thirdIdent = new BigInteger(bThirdIdent);

			BigInteger firstSecret = new BigInteger(bFirstSecret);
			BigInteger secondSecret = new BigInteger(bSecondSecret);
			BigInteger thirdSecret = new BigInteger(bThirdSecret);

			if (!firstIdent.equals(secondIdent)
					|| !secondIdent.equals(thirdIdent)) {
				throw new ResultsIdentifiersMismatch();
			}

			if (condition == Equal) {
				SharemindSharedSecret secretResult = new SharemindSharedSecret(
						1, firstSecret, secondSecret, thirdSecret);

				int result = secretResult.unshare().intValue();

				if (result == 1) {
					resultIDS.add(Boolean.TRUE);
				} else {
					resultIDS.add(Boolean.FALSE);
				}

			} else if (condition == GreaterOrEqualThan) {
				SharemindSharedSecret secretResult = new SharemindSharedSecret(
						nBits + 1, firstSecret, secondSecret, thirdSecret);

				int result = secretResult.unshare().intValue();

				if (result == 0) {
					resultIDS.add(Boolean.TRUE);
				} else {
					resultIDS.add(Boolean.FALSE);
				}
			}
		}

		return resultIDS;
	}
}
