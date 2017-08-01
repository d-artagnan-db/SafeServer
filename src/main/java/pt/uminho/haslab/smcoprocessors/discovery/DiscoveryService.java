package pt.uminho.haslab.smcoprocessors.discovery;


import java.util.List;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;


public interface DiscoveryService {
    
    List<RegionLocation> discoverRegions(RequestIdentifier requestIdentifier) throws FailedRegionDiscovery;
    
}
