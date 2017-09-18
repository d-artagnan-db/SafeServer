package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;

import java.util.List;

public interface DiscoveryServiceClient {

    /**
     * After the invocation of this method the discovery service should store
     * that the player simulated by the RegionServer is processing an operation
     * for a region and that its location is in IP:PORT (OperationID,
     * RequestIdentifier) -> PlayerID:IP:PORT
     *
     * @param requestIdentifier
     */
    void sendCurrentLocationOfPlayerInRequest(
            RequestIdentifier requestIdentifier);

    /**
     * After the invocation of this method the discovery service should remove
     * the entry that a given player is simulated by the RegionServer, the
     * address of the RegionServer and the request it is processing.
     */
    void removeCurrentLocationOfPlayerInRequest(
            RequestIdentifier requestIdentifier);

    /**
     * This method attempts to retrieve the location of the RegionServers
     * processing an operation. The result should either be an empty list if not
     * every player has successfully setup the information or a list with two
     * RegionLocations. The two region locations are the peers of the player.
     * <p>
     * The empty list result should only happen if for some reason, maybe a
     * region split, the players processing a requestIdentifier have a different
     * state of the the RegionIdentifier.
     *
     * @return
     */
    List<RegionLocation> getPeersLocation(RequestIdentifier requestIdentifier)
            throws FailedRegionDiscovery;

}
