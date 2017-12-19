package pt.uminho.haslab.saferegions.benchmarks;

import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.Equal;
import static pt.uminho.haslab.saferegions.secretSearch.SearchCondition.Condition.GreaterOrEqualThan;
import static pt.uminho.haslab.testingutils.ValuesGenerator.randomBigInteger;

public class ProtocolBenchmark {


    private static SearchCondition.Condition getCondition(String conditionInput) {
        switch (conditionInput) {
            case "Equal":
                return Equal;
            case "GreaterOrEqualThan":
                return GreaterOrEqualThan;
        }
        throw new IllegalStateException("Input condition is not supported");
    }

    private static List<List<byte[]>> generateSecrets(int nBits, int nOperations, int nElemsPerBatch) {

        List<List<byte[]>> result = new ArrayList<List<byte[]>>();

        for (int i = 0; i < nOperations; i++) {
            List<byte[]> values = new ArrayList<byte[]>();
            for (int j = 0; j < nElemsPerBatch; j++) {
                values.add(randomBigInteger(nBits).toByteArray());
            }
            result.add(values);
        }

        return result;
    }

    public static void main(String[] args) throws IOException,
			InterruptedException {

        String protocol = args[0];
        int nBits = Integer.parseInt(args[1]);
        int nOperaions = Integer.parseInt(args[2]);
        int nElemesPerBatch = Integer.parseInt(args[3]);
        System.out.println("Evaluating protocol " + protocol + " with values on a field of size " + nBits);
        System.out.println("Testing " + nOperaions + " operations with batch size of " + nElemesPerBatch);
        SearchCondition.Condition cond = getCondition(protocol);

        List<List<byte[]>> firstValues = generateSecrets(nBits, nOperaions, nElemesPerBatch);
        List<List<byte[]>> secondValues = generateSecrets(nBits, nOperaions, nElemesPerBatch);

        List<RegionServer> servers = new ArrayList<RegionServer>();
        for (int i = 0; i < 3; i++) {
            RegionServer server = new RegionServerSim(i, cond, nBits, firstValues, secondValues);
            servers.add(server);
		}

		secondValues.clear();
		long start = System.nanoTime();
		for (RegionServer server : servers) {
			server.startRegionServer();
		}
		for (RegionServer server : servers) {
			server.stopRegionServer();
		}

        List<Long> latencies = ((RegionServerSim) servers.get(0)).getLatency();
		for(Long lat:latencies){
		    System.out.println(lat);
        }

        long end = System.nanoTime();
        long duration = TimeUnit.MILLISECONDS.convert(end - start,
                TimeUnit.NANOSECONDS);
        System.out.println("Execution time was " + duration + " milliseconds");


	}
}
