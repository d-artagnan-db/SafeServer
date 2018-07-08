package pt.uminho.haslab.saferegions.middleware.relay;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import pt.uminho.haslab.saferegions.helpers.RedisUtils;
import pt.uminho.haslab.saferegions.helpers.RegionServer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/*This test aims to validate the integration of the DiscoveryService and the PeerConnectionManager. This two components
 * are essential for the correctness of the IORelay as they enables players in different clusters to communicate. This
 * is made transparently without the SMPC having to know how the connections are being made.
 * The connection of the two classes (DiscoveryService and PeerConnectionManager) is made in the function getTargetClient
 * on the IORelay class.
 *
 * As input the test should receive a List of bindingAddress and ports, create regionServers and have this regionServers
 * connect to each other and exchange messages. Thus each region server should receive a list of target clients and
 * messages to send.
 * */
public abstract class MessageExchangeDistributedClusterTest {

    final static int DISC_SERVICE_SLEEP_TIME = 200;
    final static int DISC_SERVICE_INC_TIME = 100;
    final static int DISC_SERVICE_RETRIES = 3;
    private static final Log LOG = LogFactory
            .getLog(MessageExchangeDistributedClusterTest.class.getName());
    static int NREGIONS = 9;
    static int NMESSAGES = 10;
    final List<List<byte[]>> messagesToSend;
    final List<RegionServer> regionServers;
    final CountDownLatch serversStarted;
    final CountDownLatch totalMessagesCounter;
    private final List<String> bindingAddress;
    private final List<Integer> bindingPort;
    private final List<List<BigInteger>> requestIdentifier;
    // final CountDownLatch clientsClosed;

    public MessageExchangeDistributedClusterTest(List<String> bindingAddress,
                                                 List<Integer> bindingPort, List<List<byte[]>> messagesToSend,
                                                 List<List<BigInteger>> requestIdentifier) {

        this.bindingAddress = bindingAddress;
        this.bindingPort = bindingPort;
        this.messagesToSend = messagesToSend;
        this.requestIdentifier = requestIdentifier;

        regionServers = new ArrayList<RegionServer>();
        serversStarted = new CountDownLatch(NREGIONS);
        // It is assumed that every region sends the same number of regions.
        totalMessagesCounter = new CountDownLatch(NREGIONS
                * messagesToSend.get(0).size());
        // clientsClosed = new CountDownLatch(NREGIONS);

    }

    @BeforeClass
    public static void initializeRedisContainer() throws IOException {
        RedisUtils.initializeRedisContainer();
    }

    @Test
    public void testProtocol() throws IOException, InterruptedException {

        int playerID = 0;
        for (int i = 0; i < NREGIONS; i++) {
            // Dive the regions in group of three clusters, one for each player.
            // Each region will have the same number of
            // regions.
            regionServers.add(createRegionServer(playerID, i, bindingAddress,
                    bindingPort, messagesToSend.get(i),
                    requestIdentifier.get(i)));
            playerID += 1 % 3;
        }

        long start = System.nanoTime();

        for (RegionServer rs : regionServers) {
            rs.startRegionServer();
        }

        for (RegionServer rs : regionServers) {
            rs.stopRegionServer();
        }

        long end = System.nanoTime();
        long duration = TimeUnit.SECONDS.convert(end - start,
                TimeUnit.NANOSECONDS);
        System.out.println("Execution time was " + duration + " second");
        validateResults();

        // wait some time before next tests
        Thread.sleep(5000);
    }

    protected abstract void validateResults();

    protected abstract RegionServer createRegionServer(int playerID, int index,
                                                       List<String> bindingAddress, List<Integer> bindingPort,
                                                       List<byte[]> messagesToSend, List<BigInteger> requestIdentifier)
            throws IOException;

}
