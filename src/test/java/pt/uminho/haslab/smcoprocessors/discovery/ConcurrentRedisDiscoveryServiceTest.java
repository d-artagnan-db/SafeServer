package pt.uminho.haslab.smcoprocessors.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.middleware.TestLinkedRegions;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
public abstract class ConcurrentRedisDiscoveryServiceTest
        extends
        TestLinkedRegions {

    protected static final Log LOG = LogFactory
            .getLog(ConcurrentRedisDiscoveryServiceTest.class.getName());

    protected final static int DISC_SERVICE_SLEEP_TIME = 200;
    protected final static int DISC_SERVICE_INC_TIME = 100;
    protected final static int DISC_SERVICE_RETRIES = 3;

    protected final Map<Integer, List<RegionLocation>> locations;
    protected final BigInteger requestsID;
    protected final BigInteger regionsID;
    protected final List<String> ips;
    protected final List<Integer> ports;

    public ConcurrentRedisDiscoveryServiceTest(BigInteger requestID,
                                               BigInteger regionID, List<String> ip, List<Integer> port)
            throws IOException {
        this.requestsID = requestID;
        this.regionsID = regionID;
        this.ips = ip;
        this.ports = port;
        locations = new ConcurrentHashMap<Integer, List<RegionLocation>>();
    }

    @Parameterized.Parameters
    public static Collection nbitsValues() {
        return ValuesGenerator.RedisTestValueGenerator();
    }

    protected void validateResults() throws InvalidSecretValue {
        // System.out.println("Number of lists " + locations.size());
        for (List<RegionLocation> loc : locations.values()) {
            LOG.debug("List Size " + loc.size());
            // System.out.println( "List Size " + loc.size());
            assertEquals(2, loc.size());
        }

        for (Integer player : locations.keySet()) {

            boolean allMatch = true;

            for (RegionLocation location : locations.get(player)) {
                boolean foundMatch = false;

                int index = player;
                for (int i = 0; i < 2; i++) {
                    index = (index + 1) % 3;
                    LOG.debug(player + " Comparing " + ips.get(index) + " : "
                            + ports.get(index) + " <=> " + location.getIp()
                            + ":" + location.getPort());
                    // System.out.println( ips.get(index) + " : " +
                    // ports.get(index) + " <=> " +
                    // location.getIp()+":"+location.getPort());
                    if (ips.get(index).equals(location.getIp())
                            && ports.get(index) == location.getPort()) {
                        // System.out.println(player + " Found match");
                        foundMatch |= true;
                        break;
                    }

                }
                // System.out.println(player+" FoundMatch "+ foundMatch);
                allMatch &= foundMatch;

            }

            assertEquals(true, allMatch);

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
            // System.out.println("Creating player id " + playerID);
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