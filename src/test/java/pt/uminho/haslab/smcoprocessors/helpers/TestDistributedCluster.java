package pt.uminho.haslab.smcoprocessors.helpers;

import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.benchmarks.RegionServer;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class TestDistributedCluster {

    /**
     * RegionServer is not the best name for the abstraction. It should be just
     * HBase Region. This class simulates a distributed SafeRegion architecture
     * where thee three clusters have the same number of HBase Regions. Nothing
     * is said about the number of RegionServers, that should be transparent and
     * handle any possible combination.
     * <p>
     * To keep up with the RegionServer abstraction, we assume that there is a
     * bijection between a RegionSever and a HRegion.
     */

    private final List<RegionServer> clusterOne;

    private final List<RegionServer> clusterTwo;

    private final List<RegionServer> clusterThree;

    private final int numberOfRegions;

    public TestDistributedCluster(int numberOfRegions) {
        this.clusterOne = new ArrayList<RegionServer>();
        this.clusterTwo = new ArrayList<RegionServer>();
        this.clusterThree = new ArrayList<RegionServer>();
        this.numberOfRegions = numberOfRegions;
    }

    @Test
    public void testProtocol() throws InterruptedException, InvalidSecretValue,
            IOException {

        for (int i = 0; i < numberOfRegions; i++) {
            clusterOne.add(createRegionServer(0, i));
            clusterTwo.add(createRegionServer(1, i));
            clusterThree.add(createRegionServer(2, i));
        }

        long start = System.nanoTime();

        for (int i = 0; i < numberOfRegions; i++) {
            clusterOne.get(i).startRegionServer();
            clusterTwo.get(i).startRegionServer();
            clusterThree.get(i).startRegionServer();
        }
        Thread.sleep(400);

        for (int i = 0; i < numberOfRegions; i++) {
            clusterOne.get(i).stopRegionServer();
            clusterTwo.get(i).stopRegionServer();
            clusterThree.get(i).stopRegionServer();
        }
        long end = System.nanoTime();
        long duration = TimeUnit.SECONDS.convert(end - start,
                TimeUnit.NANOSECONDS);
        System.out.println("Execution time was " + duration + " second");
        validateResults();

        // wait some time before next tests
        Thread.sleep(5000);

    }

    protected abstract RegionServer createRegionServer(int playerID, int pos)
            throws IOException;

    protected abstract void validateResults() throws InvalidSecretValue;

    protected RegionServer getRegionServer(int playerID, int pos) {

        switch (playerID) {

            case 0:
                return clusterOne.get(pos);
            case 1:
                return clusterTwo.get(pos);
            case 2:
                return clusterThree.get(pos);
            default:
                throw new IllegalStateException("Invalid Player ID");
        }
    }

}
