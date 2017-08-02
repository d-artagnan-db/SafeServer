package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;

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
                    DISC_SERVICE_INC_TIME, DISC_SERVICE_RETRIES);
            RequestIdentifier reqi = new RequestIdentifier(requestID, regionID);
            List<RegionLocation> playerLocations = null;
            try {
                playerLocations = service.discoverRegions(reqi);
            } catch (FailedRegionDiscovery failedRegionDiscovery) {
                failedRegionDiscovery.printStackTrace();
            }
            locations.put(playerID, playerLocations);
            runStatus = false;
            // System.out.println("All values put");

        }
    }
}
