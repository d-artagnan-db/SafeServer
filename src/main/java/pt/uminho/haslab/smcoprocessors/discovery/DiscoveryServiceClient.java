package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;

import java.util.List;


public interface DiscoveryServiceClient {
    
    /**
     * After the invocation of this method the discovery service 
     * should store that the player simulated by the RegionServer is processing
     * an operation for a region and that its location is in IP:PORT
     * (OperationID, RequestIdentifier) -> PlayerID:IP:PORT
     * @param requestIdentifier
     */
     void sendCurrentLocationOfPlayerInRequest(RequestIdentifier requestIdentifier);
    
    /**
     * This method attempts to retrieve the location of the RegionServers 
     * processing an operation. The result should either be an empty list if not
     * every player has successfully setup the information or a list with two 
     * RegionLocations. The two region locations are the peers of the player.
     * 
     * The empty list result should only happen if for some reason, maybe a 
     * region split, the players processing a requestIdentifier have a different
     * state of the the RegionIdentifier.
     * @return 
     */
     List<RegionLocation> getPeersLocation();
    
    
}