package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Not;

public class UnarySearchValue extends AbstractSearchValue {
    static final Log LOG = LogFactory.getLog(UnarySearchValue.class.getName());

    private final SearchCondition searchCondition;

    public UnarySearchValue(Condition condition,
                            SearchCondition searchCondition, int targetPlayer) {
        // TODO: check if condition is on the same column;
        super(condition, targetPlayer);
        this.searchCondition = searchCondition;
    }

    public List<Boolean> evaluateCondition(List<byte[]> value,
                                           List<byte[]> rowID, SharemindPlayer p) {
        List<Boolean> results = new ArrayList<Boolean>();

        if (condition == Not) {
            List<Boolean> vals = searchCondition.evaluateCondition(value,
                    rowID, p);

            for (Boolean val : vals) {
                results.add(!val);
            }
        }

        return results;
    }

    public Condition getCondition() {
        return searchCondition.getCondition();
    }
}
