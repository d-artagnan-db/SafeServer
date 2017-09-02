package pt.uminho.haslab.smcoprocessors.middleware.relay;

import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.RegionServer;
import pt.uminho.haslab.smcoprocessors.middleware.helpers.TestRegionServer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RelaySetupTest {

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
