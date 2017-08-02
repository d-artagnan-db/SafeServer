package pt.uminho.haslab.smcoprocessors.SecretSearch;

import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smcoprocessors.protocolresults.SearchResults;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.*;

public abstract class AbstractSearchValue implements SearchCondition {

    public static SearchCondition conditionTransformer(Condition op, int nBits,
                                                       List<byte[]> value, int targetPlayer) {
        switch (op) {
            case Equal:
                return new SearchValue(nBits, value, Equal, targetPlayer);
            case GreaterOrEqualThan:
                return new SearchValue(nBits, value, GreaterOrEqualThan,
                        targetPlayer);
            case Greater:
                SearchCondition equal = new SearchValue(nBits, value, Equal,
                        targetPlayer);
                SearchCondition notEqual = new UnarySearchValue(Not, equal,
                        targetPlayer);
                SearchCondition greaterEqualThan = new SearchValue(nBits,
                        value, GreaterOrEqualThan, targetPlayer);
                return new ComposedSearchValue(And, notEqual, greaterEqualThan,
                        targetPlayer);
            case Less:
                greaterEqualThan = new SearchValue(nBits, value,
                        GreaterOrEqualThan, targetPlayer);
                return new UnarySearchValue(Not, greaterEqualThan, targetPlayer);
            case LessOrEqualThan:
                greaterEqualThan = new SearchValue(nBits, value,
                        GreaterOrEqualThan, targetPlayer);
                equal = new SearchValue(nBits, value, Equal, targetPlayer);
                SearchCondition notGreater = new UnarySearchValue(Not,
                        greaterEqualThan, targetPlayer);
                return new ComposedSearchValue(Or, equal, notGreater,
                        targetPlayer);
            case NotEqual:
                equal = new SearchValue(nBits, value, Equal, targetPlayer);
                return new UnarySearchValue(Not, equal, targetPlayer);

        }
        return null;

    }

    protected final Condition condition;
    protected final int targetPlayer;

    public AbstractSearchValue(Condition condition, int targetPlayer) {
        this.condition = condition;
        this.targetPlayer = targetPlayer;
    }

    public Condition getCompare() {
        return condition;
    }

    protected SearchResults createSearchResults(Secret secret, byte[] id)
            throws ResultsLengthMissmatch {
        List<byte[]> secrets = new ArrayList<byte[]>();
        List<byte[]> ids = new ArrayList<byte[]>();

        ids.add(id);
        secrets.add(((SharemindSecret) secret).getValue().toByteArray());

        return new SearchResults(secrets, ids);

    }

    protected SearchResults createBatchSearchResults(List<byte[]> secrets,
                                                     List<byte[]> ids) throws ResultsLengthMissmatch {
        return new SearchResults(secrets, ids);

    }

}
