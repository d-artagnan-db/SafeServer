package pt.uminho.haslab.saferegions.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.protocolresults.ResultsLengthMismatch;
import pt.uminho.haslab.smpc.interfaces.Secret;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSecret;

import java.util.ArrayList;
import java.util.List;


public abstract class AbstractSearchValue implements SearchCondition {

    static final Log LOG = LogFactory.getLog(AbstractSearchValue.class
            .getName());

    protected final Condition condition;

    public AbstractSearchValue(Condition condition) {
        this.condition = condition;
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
