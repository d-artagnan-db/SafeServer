package pt.uminho.haslab.smcoprocessors.benchmarks;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.SecretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.smcoprocessors.SecretSearch.SearchCondition.Condition.Equal;

/***
 * Simulator of a region server used to benchmark the SMPC protocols
 */
public class RegionServerSim extends TestRegionServer {
    private final static SecureRandom generator = new SecureRandom();

    private final int runTime;

    public RegionServerSim(int playerID, int runTime) throws IOException {

        super(playerID);
        this.runTime = runTime;

    }

    private SearchCondition getSearchCondition(Condition cond, int nBits,
                                               byte[] secTwo) {
        List<byte[]> secTwos = new ArrayList<byte[]>();
        secTwos.add(secTwo);
        return AbstractSearchValue.conditionTransformer(cond, nBits + 1,
                secTwos, 1);
    }

    public int getMSent(TestPlayer player) {
        int mSent = 0;
        Map<Integer, List<BigInteger>> messagesSent = player.getMessagesSent();

        // System.out.println("Nbits is "+nBits+1);
        for (Integer keys : messagesSent.keySet()) {
            mSent += messagesSent.get(keys).size();
        }
        return mSent;
    }

    @Override
    public void doComputation() {
        // Condition[] conds = {Equal, GreaterOrEqualThan, Greater,
        // LessOrEqualThan, Less};
        Condition[] conds = {Equal};

        // int[] nbits = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
        int[] nbits = {2};

        long reqID = 1;
        long regionID = 1;

        for (Condition cond : conds) {
            for (int nbit : nbits) {

                double elapsed = 0;
                long nops = 0;
                long start = System.nanoTime();

                while (elapsed < runTime) {

                    byte[] breqID = ("" + reqID).getBytes();
                    byte[] bregionID = ("" + regionID).getBytes();
                    RequestIdentifier ident = new RequestIdentifier(breqID,
                            bregionID);
                    TestPlayer player = new TestPlayer(relay, ident, playerID,
                            broker);
                    if (this.playerID == 1) {
                        player.setTargetPlayer();
                    }

                    BigInteger valOne = new BigInteger(nbit, generator);
                    BigInteger valTwo = new BigInteger(nbit, generator);

                    List<byte[]> valsOne = new ArrayList<byte[]>();
                    List<byte[]> ids = new ArrayList<byte[]>();
                    valsOne.add(valOne.toByteArray());
                    ids.add(breqID);
                    SearchCondition scond = getSearchCondition(cond, nbit,
                            valTwo.toByteArray());

                    scond.evaluateCondition(valsOne, ids, player);
                    reqID += 1;
                    regionID += 1;
                    long stop = System.nanoTime();

                    elapsed = (stop - start) / 1000000000;
                    nops += 1;
                    System.out.println("elapsed " + elapsed);
                    System.out.println("nops " + nops);
                    broker.allBatchMessagesRead(ident);

                }

            }

        }

    }

}
