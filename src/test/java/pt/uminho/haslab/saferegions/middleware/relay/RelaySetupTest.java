package pt.uminho.haslab.saferegions.middleware.relay;

import org.junit.BeforeClass;
import org.junit.Test;
import pt.uminho.haslab.saferegions.helpers.RedisUtils;
import pt.uminho.haslab.saferegions.helpers.RegionServer;
import pt.uminho.haslab.saferegions.helpers.TestRegionServer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RelaySetupTest {

    @BeforeClass
    public static void initializeRedisContainer() throws IOException {
        RedisUtils.initializeRedisContainer();
    }

    @Test
    public void connectRegionServers() throws InterruptedException, IOException {
        RegionServer a = new RegionServerImpl(0);
        RegionServer b = new RegionServerImpl(1);
        RegionServer c = new RegionServerImpl(2);

        a.startRegionServer();
        b.startRegionServer();
        c.startRegionServer();

        a.stopRegionServer();
        b.stopRegionServer();
        c.stopRegionServer();

        assertEquals(true,
                a.getRunStatus() & b.getRunStatus() & c.getRunStatus());

    }

    private class RegionServerImpl extends TestRegionServer {

        public RegionServerImpl(int playerID) throws IOException {
            super(playerID);
        }

        @Override
        public void doComputation() {
        }

    }

}
