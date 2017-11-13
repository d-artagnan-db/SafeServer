package pt.uminho.haslab.saferegions.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import pt.uminho.haslab.saferegions.benchmarks.RegionServer;
import pt.uminho.haslab.saferegions.helpers.RedisUtils;
import pt.uminho.haslab.saferegions.helpers.TestDistributedCluster;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.TestCase.assertEquals;

public abstract class ConcurrentNRegionsRedisDiscService
		extends
			TestDistributedCluster {

	protected static final Log LOG = LogFactory
			.getLog(ConcurrentNRegionsRedisDiscService.class.getName());

	protected final static int DISC_SERVICE_SLEEP_TIME = 2000;
	protected final static int DISC_SERVICE_INC_TIME = 1000;
	protected final static int DISC_SERVICE_RETRIES = 10;

	protected final Map<Integer, List<BigInteger>> requestIDs;
	protected final Map<Integer, List<BigInteger>> regionIDs;
	protected final Map<Integer, List<String>> ips;
	protected final Map<Integer, List<Integer>> ports;

	protected final Map<Integer, Map<Integer, List<RegionLocation>>> locations;

	public ConcurrentNRegionsRedisDiscService(
			Map<Integer, List<BigInteger>> requestIDs,
			Map<Integer, List<BigInteger>> regionIDs,
			Map<Integer, List<String>> ips, Map<Integer, List<Integer>> ports) {
		super(requestIDs.get(0).size());

		this.requestIDs = requestIDs;
		this.regionIDs = regionIDs;
		this.ips = ips;
		this.ports = ports;
		locations = new ConcurrentHashMap<Integer, Map<Integer, List<RegionLocation>>>();
		locations.put(0, new HashMap<Integer, List<RegionLocation>>());
		locations.put(1, new HashMap<Integer, List<RegionLocation>>());
		locations.put(2, new HashMap<Integer, List<RegionLocation>>());

	}

    @BeforeClass
    public static void initializeRedisContainer() throws IOException {
        RedisUtils.initializeRedisContainer();
    }

    protected void validateResults() throws InvalidSecretValue {

		for (Integer playerID : locations.keySet()) {

			for (Integer pos : locations.get(playerID).keySet()) {

				List<RegionLocation> receivedLocations = locations
						.get(playerID).get(pos);
				assertEquals(2, locations.get(playerID).get(pos).size());

				boolean allMatch = true;

				for (RegionLocation location : receivedLocations) {
					boolean foundMatch = false;
					int index = playerID;
					for (int i = 0; i < 2; i++) {

						index = (index + 1) % 3;

						String ip = ips.get(index).get(pos);
						Integer port = ports.get(index).get(pos);

						if (ip.equals(location.getIp())
								&& port == location.getPort()) {
							foundMatch |= true;
						}
					}
					allMatch &= foundMatch;
				}

				assertEquals(true, allMatch);

			}
		}
	}

	protected abstract class RedisRegionServer extends Thread
			implements
				RegionServer {
		protected final int playerID;
		protected final byte[] requestID;
		protected final byte[] regionID;
		protected final String ip;
		protected final Integer port;
		protected boolean runStatus;

		public RedisRegionServer(int playerID, byte[] requestID,
				byte[] regionID, String ip, Integer port) {
			LOG.debug("Creating player id " + playerID);
			this.playerID = playerID;
			this.requestID = requestID;
			this.regionID = regionID;
			this.ip = ip;
			this.port = port;
			runStatus = true;
		}

		public void startRegionServer() {
			this.start();

		}

		public void stopRegionServer() throws IOException, InterruptedException {
			this.join();

		}

		public boolean getRunStatus() {
			return runStatus;
		}

	}
}
