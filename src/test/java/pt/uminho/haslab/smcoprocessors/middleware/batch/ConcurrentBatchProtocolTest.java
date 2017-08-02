package pt.uminho.haslab.smcoprocessors.middleware.batch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.MessageBroker;
import pt.uminho.haslab.smcoprocessors.CMiddleware.Relay;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.ConcurrentProtocolTest;
import pt.uminho.haslab.smcoprocessors.middleware.TestLinkedRegions;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.ConcurrentBatchTestPlayer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(Parameterized.class)
public abstract class ConcurrentBatchProtocolTest extends TestLinkedRegions {

    private static final Log LOG = LogFactory
            .getLog(ConcurrentProtocolTest.class.getName());

    protected final List<Integer> nbits;
    protected final List<List<BigInteger>> valuesOne;
    protected final List<List<BigInteger>> valuesTwo;
    protected final Map<Integer, List<List<byte[]>>> secretsOne;
    protected final Map<Integer, List<List<byte[]>>> secretsTwo;

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.TwoBatchValuesGenerator();
    }

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

    @Override
    protected RegionServer createRegionServer(int playerID) throws IOException {
        return new RSImpl(playerID, secretsOne.get(playerID),
                secretsTwo.get(playerID), nbits);
    }

    protected abstract ConcurrentBatchTestPlayer createConcurrentPlayer(
            Relay relay, RequestIdentifier requestID, int playerID,
            MessageBroker broker, List<byte[]> firstValueSecret,
            List<byte[]> secondValueSecret, int nBits);

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
                ConcurrentBatchTestPlayer player = createConcurrentPlayer(
                        relay, ident, playerID, broker, secretOne, secretTwo,
                        valueNbits);

                requests.add(player);
            }

            for (ConcurrentBatchTestPlayer p : requests) {
                p.start();
            }

            for (ConcurrentBatchTestPlayer p : requests) {
                try {
                    p.join();
                } catch (InterruptedException ex) {
                    LOG.debug(ex);
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

}
