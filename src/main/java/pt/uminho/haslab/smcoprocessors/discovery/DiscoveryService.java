package pt.uminho.haslab.smcoprocessors.discovery;

import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;

import java.util.List;

public interface DiscoveryService {

    List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier)
            throws FailedRegionDiscovery;

}
