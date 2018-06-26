package pt.uminho.haslab.saferegions.discovery;

import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import pt.uminho.haslab.saferegions.helpers.RegionServer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class ConcurrentSucessRedisDiscoveryServiceTest
        extends
        ConcurrentRedisDiscoveryServiceTest {

    public ConcurrentSucessRedisDiscoveryServiceTest(BigInteger requestID,
                                                     BigInteger regionID, List<String> ip, List<Integer> port)
            throws IOException {
        super(requestID, regionID, ip, port);
    }

    protected RegionServer createRegionServer(int playerID) throws IOException {
        return new SuccRedisRegionServer(playerID, requestsID.toByteArray(),
                regionsID.toByteArray(), ips.get(playerID), ports.get(playerID));
    }

    private class SuccRedisRegionServer extends RedisRegionServer {

        public SuccRedisRegionServer(int playerID, byte[] requestID,
                                     byte[] regionID, String ip, Integer port) {
            super(playerID, requestID, regionID, ip, port);
        }

        @Override
        public void run() {
            RedisDiscoveryService service = new RedisDiscoveryService(
                    "localhost", playerID, ip, port, DISC_SERVICE_SLEEP_TIME,
                    DISC_SERVICE_INC_TIME, DISC_SERVICE_RETRIES, false);
            RequestIdentifier reqi = new RequestIdentifier(requestID, regionID);
            service.registerRegion(reqi);
            List<RegionLocation> playerLocations = null;
            try {
                playerLocations = service.discoverRegions(reqi);
            } catch (FailedRegionDiscovery failedRegionDiscovery) {
                failedRegionDiscovery.printStackTrace();
            }
            locations.put(playerID, playerLocations);
            runStatus = false;
            service.unregisterRegion(reqi);

        }
    }
}
