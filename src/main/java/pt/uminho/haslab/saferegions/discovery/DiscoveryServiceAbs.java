package pt.uminho.haslab.saferegions.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;

import java.util.List;

public abstract class DiscoveryServiceAbs implements DiscoveryService {
    private static final Log LOG = LogFactory.getLog(DiscoveryServiceAbs.class.getName());

    protected final int playerID;
    protected final int port;
    protected final String locationMessage;
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
