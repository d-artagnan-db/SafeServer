package pt.uminho.haslab.saferegions.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;
import redis.clients.jedis.Jedis;

import java.util.*;

public class RedisDiscoveryService extends DiscoveryServiceAbs {

    private static final Log LOG = LogFactory
            .getLog(RedisDiscoveryService.class.getName());
    private final Jedis jedis;
    private final DiscoveryServiceClient client;
    private final int sleepTime;
    private final int incTime;
    private final int retries;
    private final boolean fixedRegions;

    //Cache of region Locations. Region ID -> Region Locaiton
    private final Map<String, List<RegionLocation>> regionsCache;

    public RedisDiscoveryService(DiscoveryServiceConfiguration conf) {
        super(conf.getDiscoveryServiceLocation(), conf.getPlayerID(), conf
                .getRegionServerIP(), conf.getPort());
        fixedRegions = conf.areRegionsFixed();
        regionsCache = new HashMap<String, List<RegionLocation>>();
        jedis = new Jedis(conf.getDiscoveryServiceLocation());
        client = new Client();
        this.sleepTime = conf.getSleepTime();
        this.incTime = conf.getIncTime();
        this.retries = conf.getRetries();

    }

    public RedisDiscoveryService(String discoveryServiceLocation, int playerID,
                                 String regionServerIP, int port, int sleepTime, int incTime,
                                 int retries, boolean fixedRegions) {
        super(discoveryServiceLocation, playerID, regionServerIP, port);
        this.fixedRegions = fixedRegions;
        regionsCache = new HashMap<String, List<RegionLocation>>();
        jedis = new Jedis(discoveryServiceLocation);
        client = new Client();
        this.sleepTime = sleepTime;
        this.incTime = incTime;
        this.retries = retries;
    }

    public void closeConnection() {
        jedis.close();
    }

    @Override
    protected DiscoveryServiceClient getDiscoveryServiceClient() {
        return client;
    }

    private class Client implements DiscoveryServiceClient {

        private String getKey(RequestIdentifier requestIdentifier) {
            String requestID = Arrays
                    .toString(requestIdentifier.getRequestID());
            String regionID = new String(requestIdentifier.getRegionID());
            return requestID + ":" + regionID;
        }

        public synchronized void sendCurrentLocationOfPlayerInRequest(
                RequestIdentifier requestIdentifier) {

            String key = getKey(requestIdentifier);
            String regionID = Arrays.toString(requestIdentifier.getRegionID());
            if (!(fixedRegions && regionsCache.containsKey(regionID))) {
                try {
                    //LOG.debug("Pushing key "+ key + " -> "+ locationMessage);

                    jedis.lpush(key, locationMessage);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                    throw new IllegalStateException(ex);
                }
            }

        }

        public synchronized void removeCurrentLocationOfPlayerInRequest(
                RequestIdentifier requestIdentifier) {
            String key = getKey(requestIdentifier);
            jedis.del(key);
        }

        private List<RegionLocation> parseJedisResult(String regionID, List<String> results)
                throws FailedRegionDiscovery {
            /**
             * The result size should either be 0 because no jedis. lrange was
             * successful or 3 when it was successful.
             */
            List<RegionLocation> regionResult = new ArrayList<RegionLocation>();



            for (String result : results) {
                String[] subs = result.split(":");

                if (Integer.parseInt(subs[0]) != playerID) {
                    RegionLocation rl = new RegionLocation(
                            Integer.parseInt(subs[0]), subs[1],
                            Integer.parseInt(subs[2]));
                    regionResult.add(rl);
                    //LOG.debug("regions " + Integer.parseInt(subs[0]) + " <-> " + subs[1] + " <-> "+subs[2]);
                }
            }

            if (results.size() != 3) {
                String msg = "Illegal results input size: " + results.size();
                LOG.error(msg);
                throw new FailedRegionDiscovery(msg);
            }
            regionsCache.put(regionID, regionResult);
            return regionResult;
        }

        public List<RegionLocation> getPeersLocation(
                RequestIdentifier requestIdentifier)
                throws FailedRegionDiscovery {
            boolean run = true;
            List<String> clients = new ArrayList<String>();
            String key = getKey(requestIdentifier);
            //LOG.debug("Getting peer location for request " + key);
            int sleepTimeInc = sleepTime;
            int nAttempts = 0;
            String regionID = Arrays
                    .toString(requestIdentifier.getRegionID());
            if (fixedRegions && regionsCache.containsKey(regionID)) {
                //  LOG.debug("Retrieved region location from cache for request " + key);
                return regionsCache.get(regionID);
            }
            synchronized (this) {
                if (fixedRegions && !regionsCache.containsKey(regionID)) {
                    while (run) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("getting key " + key);
                        }

                        clients = jedis.lrange(key, 0, -1);

                        if (clients.size() >= 3) {
                            run = false;
                        } else if (nAttempts >= retries) {
                            run = false;
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Searching for key " + sleepTimeInc);
                            }
                            try {
                                Thread.sleep(sleepTimeInc);
                                nAttempts += 1;
                                sleepTimeInc += incTime;
                            } catch (InterruptedException ex) {
                                LOG.error(ex);
                                throw new IllegalStateException(ex);

                            }
                        }
                    }
                    return parseJedisResult(regionID, clients);
                } else {
                    return regionsCache.get(regionID);
                }
            }
        }

    }

}
