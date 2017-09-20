package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;

import java.util.List;

public abstract class DiscoveryServiceAbs implements DiscoveryService {

	protected final int playerID;
	protected final int port;
	final String locationMessage;
	private final String RegionServerIP;
	private final String discoveryServeLocation;

	DiscoveryServiceAbs(String discoveryServiceLocation, int playerID,
			String regionServerIP, int port) {
		this.discoveryServeLocation = discoveryServiceLocation;
		this.playerID = playerID;
		this.RegionServerIP = regionServerIP;
		this.port = port;
		this.locationMessage = playerID + ":" + regionServerIP + ":" + port;
	}

	protected abstract DiscoveryServiceClient getDiscoveryServiceClient();

	public void registerRegion(RequestIdentifier requestIdentifier) {
		getDiscoveryServiceClient().sendCurrentLocationOfPlayerInRequest(
				requestIdentifier);
	}

	public void unregisterRegion(RequestIdentifier requestIdentifier) {
		getDiscoveryServiceClient().removeCurrentLocationOfPlayerInRequest(
				requestIdentifier);
	}

	public List<RegionLocation> discoverRegions(
			RequestIdentifier requestIdentifier) throws FailedRegionDiscovery {
		return getDiscoveryServiceClient().getPeersLocation(requestIdentifier);
	}

}
