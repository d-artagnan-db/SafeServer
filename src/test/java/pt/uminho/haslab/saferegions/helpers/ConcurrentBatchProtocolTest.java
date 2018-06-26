package pt.uminho.haslab.saferegions.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.interfaces.Dealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.Assert.assertEquals;


@RunWith(Parameterized.class)
public abstract class ConcurrentBatchProtocolTest extends TestLinkedRegions {

    private static final Log LOG = LogFactory
            .getLog(ConcurrentBatchProtocolTest.class.getName());

    protected final List<Integer> nbits;
    protected final List<List<BigInteger>> valuesOne;
    protected final List<List<BigInteger>> valuesTwo;
    private final Map<Integer, List<List<byte[]>>> secretsOne;
    private final Map<Integer, List<List<byte[]>>> secretsTwo;

    public ConcurrentBatchProtocolTest(List<Integer> nbits,
                                       List<List<BigInteger>> valuesOne, List<List<BigInteger>> valuesTwo)
            throws IOException, InvalidNumberOfBits, InvalidSecretValue {

        super();
        this.nbits = nbits;
        this.valuesOne = valuesOne;
        this.valuesTwo = valuesTwo;
        secretsOne = new ConcurrentHashMap<Integer, List<List<byte[]>>>();
        secretsTwo = new ConcurrentHashMap<Integer, List<List<byte[]>>>();

        for (int i = 0; i < nbits.size(); i++) {
            if (i == 0) {
                secretsOne.put(0, new ArrayList<List<byte[]>>());
                secretsOne.put(1, new ArrayList<List<byte[]>>());
                secretsOne.put(2, new ArrayList<List<byte[]>>());

                secretsTwo.put(0, new ArrayList<List<byte[]>>());
                secretsTwo.put(1, new ArrayList<List<byte[]>>());
                secretsTwo.put(2, new ArrayList<List<byte[]>>());

            }
            Dealer dealer = new SharemindDealer(nbits.get(i));

            List<byte[]> secretsOneU1 = new ArrayList<byte[]>();
            List<byte[]> secretsOneU2 = new ArrayList<byte[]>();
            List<byte[]> secretsOneU3 = new ArrayList<byte[]>();

            List<byte[]> secretsTwoU1 = new ArrayList<byte[]>();
            List<byte[]> secretsTwoU2 = new ArrayList<byte[]>();
            List<byte[]> secretsTwoU3 = new ArrayList<byte[]>();

            for (int j = 0; j < valuesOne.get(i).size(); j++) {

                BigInteger valueOne = valuesOne.get(i).get(j);
                BigInteger valueTwo = valuesTwo.get(i).get(j);
                SharemindSharedSecret secretOne = (SharemindSharedSecret) dealer
                        .share(valueOne);
                SharemindSharedSecret secretTwo = (SharemindSharedSecret) dealer
                        .share(valueTwo);

                secretsOneU1.add(secretOne.getU1().toByteArray());
                secretsOneU2.add(secretOne.getU2().toByteArray());
                secretsOneU3.add(secretOne.getU3().toByteArray());

                secretsTwoU1.add(secretTwo.getU1().toByteArray());
                secretsTwoU2.add(secretTwo.getU2().toByteArray());
                secretsTwoU3.add(secretTwo.getU3().toByteArray());
            }

            secretsOne.get(0).add(secretsOneU1);
            secretsOne.get(1).add(secretsOneU2);
            secretsOne.get(2).add(secretsOneU3);

            secretsTwo.get(0).add(secretsTwoU1);
            secretsTwo.get(1).add(secretsTwoU2);
            secretsTwo.get(2).add(secretsTwoU3);

        }

    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.TwoBatchValuesGenerator();
    }

    @Override
    protected RegionServer createRegionServer(int playerID) throws IOException {
        return new RSImpl(playerID, secretsOne.get(playerID),
                secretsTwo.get(playerID), nbits);
    }

    protected abstract ConcurrentBatchTestPlayer createConcurrentPlayer(
            Relay relay, RequestIdentifier requestID, int playerID,
            MessageBroker broker, List<byte[]> firstValueSecret,
            List<byte[]> secondValueSecret, int nBits);

    protected abstract int getExpectedResult(BigInteger valOne,
                                             BigInteger valtwo);

    protected void validateResults() throws InvalidSecretValue {
        for (int i = 0; i < nbits.size(); i++) {

            RSImpl firstRS = (RSImpl) getRegionServer(0);
            RSImpl secondRS = (RSImpl) getRegionServer(1);
            RSImpl thirdRS = (RSImpl) getRegionServer(2);

            for (int j = 0; j < valuesOne.get(i).size(); j++) {
                BigInteger fVal = new BigInteger((firstRS).getResult(i).get(j));
                BigInteger sVal = new BigInteger((secondRS).getResult(i).get(j));
                BigInteger tVal = new BigInteger((thirdRS).getResult(i).get(j));

                SharemindSharedSecret result = new SharemindSharedSecret(1,
                        fVal, sVal, tVal);

                int expectedResult = getExpectedResult(valuesOne.get(i).get(j),
                        valuesTwo.get(i).get(j));
                assertEquals(result.unshare().intValue(), expectedResult);
            }
        }
    }

    protected class RSImpl extends TestRegionServer {

        private final int playerID;

        private final List<List<byte[]>> firstValueSecrets;

        private final List<List<byte[]>> secondValueSecrets;

        private final List<ConcurrentBatchTestPlayer> requests;

        public RSImpl(int playerID, List<List<byte[]>> firstValueSecrets,
                      List<List<byte[]>> secondValueSecrets, List<Integer> nbits)
                throws IOException {
            super(playerID);

            this.playerID = playerID;
            this.firstValueSecrets = firstValueSecrets;
            this.secondValueSecrets = secondValueSecrets;
            this.requests = new ArrayList<ConcurrentBatchTestPlayer>();

        }

        public List<byte[]> getResult(int index) {
            return requests.get(index).getResultSecret();
        }

        @Override
        public void doComputation() {
            for (int i = 0; i < nbits.size(); i++) {
                int valueNbits = nbits.get(i);

                List<byte[]> secretOne = firstValueSecrets.get(i);
                List<byte[]> secretTwo = secondValueSecrets.get(i);

                byte[] reqID = ("" + i).getBytes();
                byte[] regionID = "1".getBytes();

                RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
                relay.registerRequest(ident);
                ConcurrentBatchTestPlayer player = createConcurrentPlayer(
                        relay, ident, playerID, broker, secretOne, secretTwo,
                        valueNbits);

                requests.add(player);
            }

            for (ConcurrentBatchTestPlayer p : requests) {
                p.startProtocol();
            }

            for (ConcurrentBatchTestPlayer p : requests) {
                try {
                    p.waitEndOfProtocol();
                } catch (InterruptedException ex) {
                    LOG.debug(ex);
                    throw new IllegalStateException(ex);
                }
            }
        }
    }
}
