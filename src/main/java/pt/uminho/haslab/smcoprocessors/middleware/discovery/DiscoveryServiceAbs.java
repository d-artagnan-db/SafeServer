package pt.uminho.haslab.smcoprocessors.middleware.discovery;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;

import java.util.List;

public abstract class DiscoveryServiceAbs implements DiscoveryService {

    protected final int playerID;
    private final String RegionServerIP;
    protected final int port;
    private final String discoveryServeLocation;
    final String locationMessage;

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
        getDiscoveryServiceClient().sendCurrentLocationOfPlayerInRequest(requestIdentifier);
    }

    public void unregisterRegion(RequestIdentifier requestIdentifier) {
        getDiscoveryServiceClient().removeCurrentLocationOfPlayerInRequest(requestIdentifier);
    }

    public List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier) throws FailedRegionDiscovery {
        return getDiscoveryServiceClient().getPeersLocation(requestIdentifier);
    }

}
