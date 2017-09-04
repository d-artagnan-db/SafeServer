package pt.uminho.haslab.smcoprocessors.SecretSearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

import java.util.ArrayList;
import java.util.List;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Not;

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
                //System.out.println("Original is "+val);
                results.add(!val);
            }
            // return !searchCondition.evaluateCondition(value, rowID, p);
        } /*else {
            for (byte[] value1 : value) {
                results.add(Boolean.FALSE);
            }
        }*/

        return results;
    }

    public Condition getCondition() {
        return searchCondition.getCondition();
    }
}
