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

public class SequentialGreaterOrEqualThanProtocolTest
        extends
        DoubleValueProtocolTest {

    public SequentialGreaterOrEqualThanProtocolTest(List<Integer> nbits,
                                                    List<BigInteger> valuesOne, List<BigInteger> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {
        super(nbits, valuesOne, valuesTwo);
    }

    @Override
    protected SharemindSecret testingProtocol(Secret originalSecret,
                                              Secret cmpSecret) {
        return (SharemindSecret) originalSecret.greaterOrEqualThan(cmpSecret);
    }

    @Override
    protected void validateResults() throws InvalidSecretValue {
        for (int i = 0; i < nbits.size(); i++) {

            SharemindSharedSecret result = new SharemindSharedSecret(1,
                    protocolResults.get(0).get(i), protocolResults.get(1)
                    .get(i), protocolResults.get(2).get(i));

            int comparisonResult = valuesOne.get(i).compareTo(valuesTwo.get(i));
            int expectedResult = comparisonResult == 0 || comparisonResult == 1
                    ? 0
                    : 1;
            assertEquals(expectedResult, result.unshare().intValue());
        }
    }

}
