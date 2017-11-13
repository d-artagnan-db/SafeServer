package pt.uminho.haslab.saferegions.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProtocolBenchmark {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		Integer runTime = 60;
		List<RegionServer> servers = new ArrayList<RegionServer>();

		for (int i = 0; i < 3; i++) {
			RegionServer server = new RegionServerSim(i, runTime);
			servers.add(server);
		}
		long start = System.nanoTime();
		for (RegionServer server : servers) {
			server.startRegionServer();
		}
		Thread.sleep(400);

		for (RegionServer server : servers) {
			server.stopRegionServer();
		}
		long end = System.nanoTime();
		long duration = TimeUnit.SECONDS.convert(end - start,
				TimeUnit.NANOSECONDS);
		System.out.println("Execution time was " + duration + " second");

		// wait some time before next tests
		Thread.sleep(5000);

	}
}
