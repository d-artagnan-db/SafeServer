package pt.uminho.haslab.smcoprocessors.helpers;

import java.io.IOException;

public interface RegionServer {

	void startRegionServer();

	void stopRegionServer() throws IOException, InterruptedException;

	boolean getRunStatus();

}
