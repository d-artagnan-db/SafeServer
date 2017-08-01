package pt.uminho.haslab.smcoprocessors.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smcoprocessors.TestDistributedCluster;
import pt.uminho.haslab.smcoprocessors.benchmarks.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class ConcurrentNRegionsRedisDiscService extends TestDistributedCluster {

    protected static final Log LOG = LogFactory
            .getLog(ConcurrentNRegionsRedisDiscService.class.getName());

    protected final static int DISC_SERVICE_SLEEP_TIME = 200;
    protected final static int DISC_SERVICE_INC_TIME = 100;
    protected final static int DISC_SERVICE_RETRIES = 3;

    private final List<BigInteger> requestIDs;
    private final List<BigInteger> regionIDs;
    private final List<String> ips;
    private final List<Integer> ports;

    public ConcurrentNRegionsRedisDiscService(List<BigInteger> requestIDs, List<BigInteger> regionIDs, List<String> ips, List<Integer> ports) {
        super(requestIDs.size());

        if(!(requestIDs.size() == regionIDs.size() &&  regionIDs.size() == ips.size() && ips.size() == ports.size())){
            throw new IllegalStateException("Input lists size are not equal.");
        }

        this.requestIDs = requestIDs;
        this.regionIDs = regionIDs;
        this.ips = ips;
        this.ports = ports;
    }
    protected void validateResults() throws InvalidSecretValue {

    }


    protected abstract class RedisRegionServer extends Thread implements  RegionServer {
        protected final int playerID;
        protected final byte[] requestID;
        protected final byte[] regionID;
        protected final String ip;
        protected final Integer port;
        protected boolean runStatus;

        public RedisRegionServer(int playerID, byte[] requestID, byte[] regionID, String ip, Integer port) {
            LOG.debug("Creating player id " + playerID);
            //System.out.println("Creating player id " + playerID);
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
