package pt.uminho.haslab.smcoprocessors.discovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import pt.uminho.haslab.smcoprocessors.comunication.RequestIdentifier;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RedisDiscoveryService extends DiscoveryServiceAbs {

	private static final Log LOG = LogFactory
			.getLog(RedisDiscoveryService.class.getName());
	private final Jedis jedis;
	private final DiscoveryServiceClient client;
	private final int sleepTime;
	private final int incTime;
	private final int retries;

	public RedisDiscoveryService(DiscoveryServiceConfiguration conf) {
		super(conf.getDiscoveryServiceLocation(), conf.getPlayerID(), conf
				.getRegionServerIP(), conf.getPort());
		jedis = new Jedis(conf.getDiscoveryServiceLocation());
		client = new Client();
		this.sleepTime = conf.getSleepTime();
		this.incTime = conf.getIncTime();
		this.retries = conf.getRetries();
	}

	public RedisDiscoveryService(String discoveryServiceLocation, int playerID,
			String regionServerIP, int port, int sleepTime, int incTime,
			int retries) {
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

	private class Client implements DiscoveryServiceClient {

		private String getKey(RequestIdentifier requestIdentifier) {
			String requestID = Arrays
					.toString(requestIdentifier.getRequestID());
			String regionID = Arrays.toString(requestIdentifier.getRegionID());
			return requestID + ":" + regionID;
		}

		public synchronized void sendCurrentLocationOfPlayerInRequest(
				RequestIdentifier requestIdentifier) {
			String key = getKey(requestIdentifier);

			// LOG.debug("Going to put on redis " + key + "<->" +
			// locationMessage);
			try{
				jedis.lpush(key, locationMessage);
			}catch(Exception ex){
				LOG.debug(ex);
				throw new IllegalStateException(ex);
			}

		}

		public synchronized void removeCurrentLocationOfPlayerInRequest(
				RequestIdentifier requestIdentifier) {
			String key = getKey(requestIdentifier);
			jedis.del(key);
		}

		private List<RegionLocation> parseJedisResult(List<String> results)
				throws FailedRegionDiscovery {
			/**
			 * The result size should either be 0 because no jedis. lrange was
			 * successful or 3 when it was successful.
			 */
			List<RegionLocation> regionResult = new ArrayList<RegionLocation>();

			if (results.size() != 3) {
				String msg = "Illegal results input size: " + results.size();
				LOG.debug(msg);
				throw new FailedRegionDiscovery(msg);
			}

			for (String result : results) {
				String[] subs = result.split(":");

				if (Integer.parseInt(subs[0]) != playerID) {
					RegionLocation rl = new RegionLocation(
							Integer.parseInt(subs[0]), subs[1],
							Integer.parseInt(subs[2]));
					regionResult.add(rl);
				}
			}
			return regionResult;
		}

		public synchronized List<RegionLocation> getPeersLocation(
				RequestIdentifier requestIdentifier)
				throws FailedRegionDiscovery {
			boolean run = true;
			List<String> clients = new ArrayList<String>();
			String key = getKey(requestIdentifier);

			// LOG.debug("Going to read");
			int sleepTimeInc = sleepTime;
			int nAttempts = 0;

			while (run) {
				// LOG.debug("going to get key " + key);
				clients = jedis.lrange(key, 0, -1);

				if (clients.size() >= 3) {
					run = false;
				} else if (nAttempts >= retries) {
					run = false;
					// LOG.debug("Max retries to find peers");
				} else {
					try {
						// LOG.debug("List size " + clients.size());
						Thread.sleep(sleepTimeInc);
						nAttempts += 1;
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

}
