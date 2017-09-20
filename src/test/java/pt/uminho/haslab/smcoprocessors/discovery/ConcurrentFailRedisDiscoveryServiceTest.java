package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.helpers.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;

public class ConcurrentFailRedisDiscoveryServiceTest
		extends
			ConcurrentRedisDiscoveryServiceTest {

	private final int faultyPlayerId;
	private int faultyDiscovery;

	public ConcurrentFailRedisDiscoveryServiceTest(BigInteger requestID,
			BigInteger regionID, List<String> ip, List<Integer> port)
			throws IOException {
		super(requestID, regionID, ip, port);
		Random rand = new Random();
		// Generates a random number between 0 and 2
		faultyPlayerId = rand.nextInt(3);
		faultyDiscovery = 0;

	}

	protected RegionServer createRegionServer(int playerID) throws IOException {
		return new SuccRedisRegionServer(playerID, requestsID.toByteArray(),
				regionsID.toByteArray(), ips.get(playerID), ports.get(playerID));
	}

	@Override
	protected void validateResults() throws InvalidSecretValue {
		assertEquals(2, faultyDiscovery);
	}

	private class SuccRedisRegionServer extends RedisRegionServer {

		public SuccRedisRegionServer(int playerID, byte[] requestID,
				byte[] regionID, String ip, Integer port) {
			super(playerID, requestID, regionID, ip, port);
		}

		@Override
		public void run() {
			if (playerID != faultyPlayerId) {
				RedisDiscoveryService service = new RedisDiscoveryService(
						"localhost", playerID, ip, port,
						DISC_SERVICE_SLEEP_TIME, DISC_SERVICE_INC_TIME,
						DISC_SERVICE_RETRIES);
				RequestIdentifier reqi = new RequestIdentifier(requestID,
						regionID);
				List<RegionLocation> playerLocations = new ArrayList<RegionLocation>();
				try {
					playerLocations = service.discoverRegions(reqi);
				} catch (FailedRegionDiscovery failedRegionDiscovery) {
					// failedRegionDiscovery.printStackTrace();
					LOG.debug(failedRegionDiscovery);
					faultyDiscovery += 1;
				}

				locations.put(playerID, playerLocations);
			}
			runStatus = false;

		}
	}

}
