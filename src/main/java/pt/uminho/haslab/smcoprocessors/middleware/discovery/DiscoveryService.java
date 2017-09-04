package pt.uminho.haslab.smcoprocessors.middleware.discovery;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;

import java.util.List;

public interface DiscoveryService {

    void registerRegion(RequestIdentifier requestIdentifier);

    void unregisterRegion(RequestIdentifier requestIdentifier);

    List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier) throws FailedRegionDiscovery;

}
