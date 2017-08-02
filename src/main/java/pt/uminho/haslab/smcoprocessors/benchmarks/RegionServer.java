package pt.uminho.haslab.smcoprocessors.benchmarks;

import java.io.IOException;

public interface RegionServer {

    void startRegionServer();

    void stopRegionServer() throws IOException, InterruptedException;

    boolean getRunStatus();

}
