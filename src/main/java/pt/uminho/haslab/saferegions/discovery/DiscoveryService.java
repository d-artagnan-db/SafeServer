package pt.uminho.haslab.saferegions.discovery;

import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;

import java.util.List;

public interface DiscoveryService {

    void registerRegion(RequestIdentifier requestIdentifier);

    void unregisterRegion(RequestIdentifier requestIdentifier);

    List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier)
            throws FailedRegionDiscovery;

    void closeConnection();

}
