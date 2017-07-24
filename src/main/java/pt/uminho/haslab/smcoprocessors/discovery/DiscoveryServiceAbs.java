
package pt.uminho.haslab.smcoprocessors.discovery;

import java.util.List;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;


public abstract class DiscoveryServiceAbs implements DiscoveryService {
    
    protected final int playerID;
    protected final String RegionServerIP;
    protected final int port;
    protected final String discoveryServeLocation;
    protected final String locationMessage;
    
    public DiscoveryServiceAbs(String discoveryServiceLocation, int playerID, String regionServerIP, int port){
        this.discoveryServeLocation = discoveryServiceLocation;
        this.playerID = playerID;
        this.RegionServerIP = regionServerIP;
        this.port = port;
        this.locationMessage = playerID+":"+regionServerIP+":"+port;
    }
    
    protected abstract DiscoveryServiceClient getDiscoveryServiceClient();
    
    
    public List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier){
        DiscoveryServiceClient client  = getDiscoveryServiceClient();
        client.sendCurrentLocationOfPlayerInRequest(requestIdentifier);
        return client.getPeersLocation();
    }

    
    
    
    
    
    
    
    
    
    
}
