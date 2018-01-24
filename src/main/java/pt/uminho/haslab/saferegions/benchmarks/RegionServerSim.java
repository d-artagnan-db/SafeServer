package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.secretSearch.BigIntegerSearchConditionFactory;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***
 * Simulator of a region server used to benchmark the SMPC protocols
 */
public abstract class RegionServerSim extends TestRegionServer {

    protected final Condition cond;
    protected final int nBits;

    protected final List<List<byte[]>> firstInputs;
    protected final  List<List<byte[]>> secondInputs;
    protected final List<byte[]> ids;


    protected final List<TestPlayer> players;
    protected final List<SearchCondition> searchConditions;
    protected final List<Long> latency;

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

    protected abstract SearchCondition getSearchCondition(
            List<byte[]> secTwo);

    public List<Long> getLatency(){
        return this.latency;
    }

	@Override
	public void doComputation() {
        /** SearchConditions are being removed from the SearchConditions filter so the space used for protocol
         *  results can be freed by the JVM. The process was quickly using all of the JVM heap space.
         */
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
