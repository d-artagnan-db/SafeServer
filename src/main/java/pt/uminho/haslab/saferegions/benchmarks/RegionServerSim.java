package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.secretSearch.*;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***
 * Simulator of a region server used to benchmark the SMPC protocols
 */
public class RegionServerSim extends TestRegionServer {

    private final Condition cond;
    private final int nBits;

    private final List<List<byte[]>> firstInputs;
    private final  List<List<byte[]>> secondInputs;
    private final List<byte[]> ids;


    private final List<TestPlayer> players;
    private final List<SearchCondition> searchConditions;
    private final List<Long> latency;

    public RegionServerSim(int playerID, Condition condition, int nBits, List<List<byte[]>> firstInputs, List<List<byte[]>> secondInputs) throws IOException {

		super(playerID);
        this.cond = condition;
        this.nBits = nBits;
        this.firstInputs = firstInputs;
        this.secondInputs = secondInputs;
        this.players = new ArrayList<TestPlayer>();
        this.searchConditions = new ArrayList<SearchCondition>();
        this.ids = new ArrayList<byte[]>();
        latency = new ArrayList<Long>();
        initResources();

	}

    private void initResources() {
        byte[] regionID = "1".getBytes();
        for (int i = 0; i < firstInputs.size(); i++) {
            byte[] requestID = ("" + i).getBytes();
            RequestIdentifier ident = new RequestIdentifier(requestID,
                    regionID);

            SharemindPlayer p = new TestPlayer(relay, ident, playerID, broker);
            p.setTargetPlayer(1);
            players.add((TestPlayer) p);

            searchConditions.add(getSearchCondition(secondInputs.get(i)));
            relay.registerRequest(ident);
        }

        for (int i = 0; i < firstInputs.get(0).size(); i++) {
            byte[] requestID = ("" + i).getBytes();
            ids.add(requestID);
        }
    }

    private SearchCondition getSearchCondition(
            List<byte[]> secTwo) {

        return new BigIntegerSearchConditionFactory(cond, nBits +1, secTwo).conditionTransformer();
    }

    public List<Long> getLatency(){
        return this.latency;
    }

	@Override
	public void doComputation() {
        while(!searchConditions.isEmpty()) {
            if(playerID == 0){
                long start = System.nanoTime();
                searchConditions.get(0).evaluateCondition(firstInputs.get(0), ids, players.get(0));
                long end = System.nanoTime();
                long duration = end - start;
                latency.add(duration);
            }else{
                searchConditions.get(0).evaluateCondition(firstInputs.get(0), ids, players.get(0));
            }
            searchConditions.remove(0);
            players.remove(0);
        }
	}

}
