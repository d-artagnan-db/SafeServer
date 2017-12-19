package pt.uminho.haslab.saferegions.discovery;

public class DiscoveryServiceConfiguration {

	private final String discoveryServiceLocation;
	private final int playerID;
	private final String regionServerIP;
	private final int port;
	private final int sleepTime;
	private final int incTime;
	private final int retries;
	private final boolean fixedRegions;

    public DiscoveryServiceConfiguration(String discoveryServiceLocation,
			int playerID, String regionServerIP, int port, int sleepTime,
			int incTime, int retries, boolean fixedRegions) {
		this.discoveryServiceLocation = discoveryServiceLocation;
		this.playerID = playerID;
		this.regionServerIP = regionServerIP;
		this.port = port;
		this.sleepTime = sleepTime;
		this.incTime = incTime;
		this.retries = retries;
		this.fixedRegions = fixedRegions;
	}

	public String getDiscoveryServiceLocation() {
		return discoveryServiceLocation;
	}

	public int getPlayerID() {
		return playerID;
	}

	public String getRegionServerIP() {
		return regionServerIP;
	}

	public int getPort() {
		return port;
	}

	public int getSleepTime() {
		return sleepTime;
	}

	public int getIncTime() {
		return incTime;
	}

	public int getRetries() {
		return retries;
	}

	public boolean areRegionsFixed(){
        return fixedRegions;
    }
}
