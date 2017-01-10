package pt.uminho.haslab.smcoprocessors.middleware.helpers;

import java.io.IOException;

public interface RegionServer {

	public void startRegionServer();

	public void stopRegionServer() throws IOException, InterruptedException;

	public boolean getRunStatus();

}
