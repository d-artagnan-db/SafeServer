
package pt.uminho.haslab.smcoprocessors.discovery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.CMiddleware.RequestIdentifier;
import redis.clients.jedis.Jedis;

public class RedisDiscoveryService extends DiscoveryServiceAbs {
    
    private final Jedis jedis;
    private final DiscoveryServiceClient client;
    private final int sleepTime;
    private final int incTime;
    private final int retries;
    
    private static final Log LOG = LogFactory.getLog(RedisDiscoveryService.class.getName());

    private class Client implements DiscoveryServiceClient{

        private String key;

        public void sendCurrentLocationOfPlayerInRequest(RequestIdentifier requestIdentifier) {
            String requestID =  Arrays.toString(requestIdentifier.getRequestID());
            String regionID = Arrays.toString(requestIdentifier.getRegionID());
            key = requestID + ":"  + regionID;
            System.out.println("Going to put on redis " + key + ":" + locationMessage);
            jedis.lpush(key, locationMessage);
            
        }
        
        private List<RegionLocation> parseJedisResult(List<String> results){
            List<RegionLocation> regionResult = new ArrayList<RegionLocation>();
            /**
             * The result size should either be 0 because no jedis.lrange was successful or 3 when it was successful.
             */
            if(results.size() != 3 || results.isEmpty()){
                String msg = "Illegal results input size";
                LOG.debug(msg);
                throw new IllegalStateException(msg);
            }
            
            for(String result: results){
                String[] subs = result.split(":");
                
                if(Integer.parseInt(subs[0]) != playerID){
                    RegionLocation rl = new RegionLocation(subs[1], Integer.parseInt(subs[2]));
                    regionResult.add(rl);
                }
            }
            return regionResult;            
        }
        

        public List<RegionLocation> getPeersLocation() {
            boolean run = true;
            List<String> clients = new ArrayList<String>();
            System.out.println("Going to read");
            int sleepTimeInc = sleepTime;
            int nAttempts = 0;
            
            while(run){
                System.out.println("going to get key " + key);
                clients = jedis.lrange(key,0 ,-1);

                if(clients.size()>=3){
                    run = false;
                }else if (nAttempts >= retries){
                    run = false;
                    LOG.debug("Max retries to find peers");
                }else{
                    try {
                        System.out.println("List size "+ clients.size());
                        Thread.sleep(sleepTimeInc);
                        nAttempts +=1;
                        sleepTimeInc += incTime;
                    } catch (InterruptedException ex) {
                        LOG.debug(ex);
                        throw new IllegalStateException(ex);
                        
                    }
                }
            }
            return parseJedisResult(clients);
        }
    
    }
    
    public RedisDiscoveryService(String discoveryServiceLocation, int playerID, String regionServerIP, int port, int sleepTime, int incTime, int retries) {
        super(discoveryServiceLocation, playerID, regionServerIP, port);
        jedis = new Jedis(discoveryServiceLocation);
        client = new Client();
        this.sleepTime = sleepTime;
        this.incTime = incTime;
        this.retries = retries;
    }

    @Override
    protected DiscoveryServiceClient getDiscoveryServiceClient() {
        return client;
    }
    
}
