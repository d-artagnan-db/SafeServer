package pt.uminho.haslab.smcoprocessors.SecretSearch;

import pt.uminho.haslab.smcoprocessors.SharemindPlayer;

import java.util.List;

public interface SearchCondition {

    /**
     * The smpc library only supports by default the Equal and
     * GreaterOrEquanThan protocols.
     * <p>
     * The other comparison can be obtained by combining those two, the
     * following way: GreaterThan = !Equal && GreaterOrEqualThan LesserThan =
     * !GreaterOrEqualThan LesserOrEqualThan = !GreaterOrEqualThan && Equal
     */
    enum Condition {
        Equal, GreaterOrEqualThan, Greater, Less, LessOrEqualThan, NotEqual, And, Or, Not, Nop
    }

    /**
     * Executes the comparison of a batch of values with a smpc. An empty list
     * is returned if no match is found or a list of ids that satisfy the
     * protocol.
     *
     * @param value
     * @param rowID
     * @param p
     * @return
     */
    List<Boolean> evaluateCondition(List<byte[]> value, List<byte[]> rowID,
                                    SharemindPlayer p);

    Condition getCondition();
}
