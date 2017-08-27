package pt.uminho.haslab.smcoprocessors.discovery;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import pt.uminho.haslab.smcoprocessors.benchmarks.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public class ConcurrentNRegionsFailRedisDiscServiceTest extends ConcurrentNRegionsRedisDiscService {

    /*This class simulates a cluster where some regions fail at publishing their location.
    * In the class constructor it is selected at random some bannedPlayers and requestsIds in a
    * tuple (bannedPlayer, requestId) which are banned from sending their location to redis.
    * The tuple is maintained by the following two lists.
    * */
    private final Map<Integer, Integer> bannedPlayers;
    private final Set<Integer> bannedRequestIDs;

    private final static int nRegions = 10;
    private final static int bannedRegions = 2;
    private final Random r;
    private final AtomicInteger fails;

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.NRegionsRedisTestValueGenerator(nRegions);
    }

    public ConcurrentNRegionsFailRedisDiscServiceTest(Map<Integer, List<BigInteger>> requestIDs, Map<Integer, List<BigInteger>> regionIDs, Map<Integer, List<String>> ips, Map<Integer, List<Integer>> ports) {
        super(requestIDs, regionIDs, ips, ports);
        bannedPlayers = new HashMap<Integer, Integer>();
        bannedRequestIDs = new HashSet<Integer>();
        r = new Random();
        fails = new AtomicInteger(0);

        if (!(bannedRegions < nRegions)) {
            throw new IllegalArgumentException("Teste parameters are not valid. " +
                    "BannedRegions must be lesser than nRegions");
        }

        for (int i = 0; i < bannedRegions; i++) {

            int player = r.nextInt(3);
            int pos = r.nextInt(nRegions);
            bannedPlayers.put(pos, player);
            bannedRequestIDs.add(pos);
        }
    }

    protected RegionServer createRegionServer(int playerID, int pos) throws IOException {
        return new RedisRegionServerImpl(pos, playerID, requestIDs.get(playerID).get(pos).toByteArray(),
                regionIDs.get(playerID).get(pos).toByteArray(), ips.get(playerID).get(pos), ports.get(playerID).get(pos));
    }

    @Override
    protected void validateResults() throws InvalidSecretValue {
        assertEquals(2 * (this.bannedRequestIDs.size()), fails.get());
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
            if (!(bannedRequestIDs.contains(pos) && bannedPlayers.get(pos) == playerID)) {
                RequestIdentifier reqi = new RequestIdentifier(requestID, regionID);
                List<RegionLocation> playerLocations = null;
                service.registerRegion(reqi);

                try {
                    playerLocations = service.discoverRegions(reqi);
                } catch (FailedRegionDiscovery failedRegionDiscovery) {
                    LOG.debug(failedRegionDiscovery);
                    fails.addAndGet(1);
                }
                locations.get(playerID).put(pos, playerLocations);
                //service.unregisterRegion(reqi);
                runStatus = false;
            }

        }
    }
}
