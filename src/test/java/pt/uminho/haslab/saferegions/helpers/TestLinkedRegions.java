package pt.uminho.haslab.saferegions.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class TestLinkedRegions {

	private static final Log LOG = LogFactory.getLog(TestLinkedRegions.class
			.getName());

	private final List<RegionServer> servers;

	public TestLinkedRegions() throws IOException {
		servers = new ArrayList<RegionServer>();
	}

    @BeforeClass
    public static void initializeRedisContainer() throws IOException {
        RedisUtils.initializeRedisContainer();
    }


    @Test
	public void testProtocol() throws InterruptedException, InvalidSecretValue,
			IOException {

		for (int i = 0; i < 3; i++) {
			RegionServer server = createRegionServer(i);
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
		validateResults();

		// wait some time before next tests
		Thread.sleep(5000);

	}

	protected abstract RegionServer createRegionServer(int playerID)
			throws IOException;

	protected abstract void validateResults() throws InvalidSecretValue;

	protected RegionServer getRegionServer(int playerID) {
		return servers.get(playerID);
	}

}
