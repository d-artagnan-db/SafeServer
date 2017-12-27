package pt.uminho.haslab.saferegions.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.interfaces.Secret;
import pt.uminho.haslab.smpc.sharemindImp.SharemindSecret;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.*;

public abstract class AbstractSearchValue implements SearchCondition {

	static final Log LOG = LogFactory.getLog(AbstractSearchValue.class
			.getName());

	protected final Condition condition;

	public AbstractSearchValue(Condition condition) {
		this.condition = condition;
	}

	public static SearchCondition conditionTransformer(Condition op, int nBits,
			List<byte[]> value) {
		switch (op) {
			case Equal :
				return new SearchValue(nBits, value, Equal);
			case GreaterOrEqualThan :
				return new SearchValue(nBits, value, GreaterOrEqualThan);
			case Greater :
				SearchCondition equal = new SearchValue(nBits, value, Equal);
				SearchCondition notEqual = new UnarySearchValue(Not, equal,
						Equal);
				SearchCondition greaterEqualThan = new SearchValue(nBits,
						value, GreaterOrEqualThan);
				return new ComposedSearchValue(And, notEqual, greaterEqualThan,
						Greater);
			case Less :
				greaterEqualThan = new SearchValue(nBits, value,
						GreaterOrEqualThan);
				return new UnarySearchValue(Not, greaterEqualThan,
						GreaterOrEqualThan);
			case LessOrEqualThan :
				greaterEqualThan = new SearchValue(nBits, value,
						GreaterOrEqualThan);

				equal = new SearchValue(nBits, value, Equal);
				SearchCondition notGreater = new UnarySearchValue(Not,
						greaterEqualThan, GreaterOrEqualThan);
				return new ComposedSearchValue(Xor, equal, notGreater,
						LessOrEqualThan);
			case NotEqual :
				equal = new SearchValue(nBits, value, Equal);
				return new UnarySearchValue(Not, equal, Equal);

		}
		return null;

	}

	public Condition getCompare() {
		return condition;
	}

	protected List<byte[]> createSearchResults(Secret secret)
			throws ResultsLengthMismatch {
		List<byte[]> secrets = new ArrayList<byte[]>();

		secrets.add(((SharemindSecret) secret).getValue().toByteArray());

		return secrets;

	}

	public Condition getCondition() {
		return condition;
	}

}
