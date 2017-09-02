package pt.uminho.haslab.smcoprocessors.discovery;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.benchmarks.RegionServer;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ConcurrentNRegionsSucessRedisDiscServiceTest extends ConcurrentNRegionsRedisDiscService {

    public ConcurrentNRegionsSucessRedisDiscServiceTest(Map<Integer, List<BigInteger>> requestIDs, Map<Integer, List<BigInteger>> regionIDs, Map<Integer, List<String>> ips, Map<Integer, List<Integer>> ports) {
        super(requestIDs, regionIDs, ips, ports);
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.NRegionsRedisTestValueGenerator(10);
    }

    protected RegionServer createRegionServer(int playerID, int pos) throws IOException {
        return new RedisRegionServerImpl(pos, playerID, requestIDs.get(playerID).get(pos).toByteArray(),
                regionIDs.get(playerID).get(pos).toByteArray(), ips.get(playerID).get(pos), ports.get(playerID).get(pos));
    }


    protected class RedisRegionServerImpl extends RedisRegionServer {

        private final int pos;

        public RedisRegionServerImpl(int pos, int playerID, byte[] requestID, byte[] regionID, String ip, Integer port) {
            super(playerID, requestID, regionID, ip, port);
            this.pos = pos;
        }

        @Override
        public void run() {
            RedisDiscoveryService service = new RedisDiscoveryService(
                    "localhost", playerID, ip, port, DISC_SERVICE_SLEEP_TIME,
                    DISC_SERVICE_INC_TIME, DISC_SERVICE_RETRIES);
            RequestIdentifier reqi = new RequestIdentifier(requestID, regionID);
            service.registerRegion(reqi);
            List<RegionLocation> playerLocations = null;

            try {
                playerLocations = service.discoverRegions(reqi);
            } catch (FailedRegionDiscovery failedRegionDiscovery) {
                failedRegionDiscovery.printStackTrace();
            }
            locations.get(playerID).put(pos, playerLocations);
            runStatus = false;
            //service.unregisterRegion(reqi);

        }
    }
}
