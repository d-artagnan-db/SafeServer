package pt.uminho.haslab.smcoprocessors.middleware.relay;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.ContextPlayer;
import pt.uminho.haslab.smcoprocessors.benchmarks.TestRegionServer;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SendAndReceiveBatchMessageBrokerTest {

    private final List<List<BigInteger>> peerOne;

    private final List<List<BigInteger>> peerTwo;

    private final List<List<BigInteger>> peerThree;

    public SendAndReceiveBatchMessageBrokerTest(List<List<BigInteger>> peerOne,
                                                List<List<BigInteger>> peerTwo, List<List<BigInteger>> peerThree) {
        this.peerOne = peerOne;
        this.peerTwo = peerTwo;
        this.peerThree = peerThree;
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.SendAndReceiveBatchMessageBrokerTest2();
    }

    @Test
    public void testMessageRightRotate() throws IOException,
            InterruptedException {

        RegionServer a = new RegionServer(0, peerOne, 1, 2);

        RegionServer b = new RegionServer(1, peerTwo, 2, 0);

        RegionServer c = new RegionServer(2, peerThree, 0, 1);

        a.startRegionServer();
        b.startRegionServer();
        c.startRegionServer();
        a.stopRegionServer();
        b.stopRegionServer();
        c.stopRegionServer();

        validateMessageRightRotate(a, b, c);
        // Wait some time before next test
        Thread.sleep(10000);
    }

    @Test
    public void testMessageLeftRotate() throws IOException,
            InterruptedException {

        RegionServer a = new RegionServer(0, peerOne, 2, 1);

        RegionServer b = new RegionServer(1, peerTwo, 0, 2);

        RegionServer c = new RegionServer(2, peerThree, 1, 0);

        a.startRegionServer();
        b.startRegionServer();
        c.startRegionServer();
        a.stopRegionServer();
        b.stopRegionServer();
        c.stopRegionServer();

        validateMessageLeftRotate(a, b, c);
        // Wait some time before next test
        Thread.sleep(5000);
    }

    private void validateMessageRightRotate(RegionServer a, RegionServer b,
                                            RegionServer c) {

        assertEquals(a.recVal.size(), peerThree.size());
        assertEquals(b.recVal.size(), peerOne.size());
        assertEquals(c.recVal.size(), peerTwo.size());

        for (int i = 0; i < a.recVal.size(); i++) {

            for (int j = 0; j < peerOne.get(i).size(); j++) {
                assertEquals(peerOne.get(i).get(j), b.recVal.get(i).get(j));
                assertEquals(peerThree.get(i).get(j), a.recVal.get(i).get(j));
                assertEquals(peerTwo.get(i).get(j), c.recVal.get(i).get(j));
            }

        }
    }

    private void validateMessageLeftRotate(RegionServer a, RegionServer b,
                                           RegionServer c) {

        assertEquals(a.recVal.size(), peerTwo.size());
        assertEquals(b.recVal.size(), peerThree.size());
        assertEquals(c.recVal.size(), peerOne.size());

        for (int i = 0; i < a.recVal.size(); i++) {

            for (int j = 0; j < peerOne.get(i).size(); j++) {

                assertEquals(peerOne.get(i).get(j), c.recVal.get(i).get(j));
                assertEquals(peerThree.get(i).get(j), b.recVal.get(i).get(j));
                assertEquals(peerTwo.get(i).get(j), a.recVal.get(i).get(j));
            }

        }
    }

    private class RegionServer extends TestRegionServer {

        private final List<List<BigInteger>> toSend;

        private final List<List<BigInteger>> recVal;

        public int playerSource;
        public int playerDest;

        public RegionServer(int playerID, List<List<BigInteger>> toSend,
                            int playerDest, int playerSource) throws IOException {
            super(playerID);
            this.toSend = toSend;
            this.playerSource = playerSource;
            this.playerDest = playerDest;
            recVal = new ArrayList<List<BigInteger>>();

        }

        @Override
        public void doComputation() {
            long contextID = 0;

            for (List<BigInteger> vals : toSend) {

                List<byte[]> bvals = new ArrayList<byte[]>();

                for (BigInteger val : vals) {
                    bvals.add(val.toByteArray());
                }
                byte[] reqID = ("" + contextID).getBytes();
                byte[] regionID = "1".getBytes();
                RequestIdentifier ident = new RequestIdentifier(reqID, regionID);
                ContextPlayer r = new ContextPlayer(relay, ident, playerID,
                        broker);
                r.sendValueToPlayer(playerDest, bvals);

                List<byte[]> receivedbValues = r.getValues(playerSource);
                List<BigInteger> recValues = new ArrayList<BigInteger>();

                for (byte[] recval : receivedbValues) {
                    recValues.add(new BigInteger(recval));
                }
                recVal.add(recValues);
                // recVal.add(r.getValue(playerSource));
                contextID++;
            }
        }
    }

}
