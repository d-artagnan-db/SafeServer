package pt.uminho.haslab.smcoprocessors.middleware;

import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Secret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class SequentialEqualProtocolTest extends DoubleValueProtocolTest {

    public SequentialEqualProtocolTest(List<Integer> nbits,
                                       List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected SharemindSecret testingProtocol(Secret originalSecret,
                                              Secret cmpSecret) {
        return (SharemindSecret) originalSecret.equal(cmpSecret);
    }

    @Override
    protected void validateResults() throws InvalidSecretValue {

        for (int i = 0; i < nbits.size(); i++) {
            SharemindSharedSecret result = new SharemindSharedSecret(1,
                    protocolResults.get(0).get(i), protocolResults.get(1)
                    .get(i), protocolResults.get(2).get(i));

            boolean comparisonResult = valuesOne.get(i)
                    .equals(valuesTwo.get(i));
            int expectedResult = comparisonResult ? 1 : 0;

            assertEquals(result.unshare().intValue(), expectedResult);
        }
        validatePlayersMessagesOrder();
    }

}
