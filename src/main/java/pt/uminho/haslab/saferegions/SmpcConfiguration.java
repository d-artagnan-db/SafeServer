package pt.uminho.haslab.saferegions;

import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.saferegions.comunication.IORelay;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.discovery.DiscoveryServiceConfiguration;

import java.io.IOException;

public class SmpcConfiguration {

	// IORelay configuration
	private final int playerID;
	private final String relayHost;
	private final int relayPort;
	private final boolean isDevelopment;

	// SMPC library configuration
	private final int batchSize;
	private final int preRandomElems;

	// DiscoveryService configuration
	private final int sleepTime;
	private final int incTime;
	private final int retries;
	private final String hostname;
	private final String discoveryServiceLocation;

	private final String databaseSchemaPath;
	private final DatabaseSchema schema;

	public SmpcConfiguration(Configuration conf) {

		// IORelay configuration
		playerID = conf.getInt("smhbase.player.id", -1);
        relayHost = conf.get("smhbase.relay.host");
		relayPort = conf.getInt("smhbase.relay.port", -1);
		isDevelopment = conf.getBoolean("hbase.coprocessor.development", true);


		// SMCP library configuration
        batchSize = conf.getInt("smhbase.batch.size", 10);
        preRandomElems = conf.getInt("smhbase.smpc.prerandom.size", 0);

		// DiscoveryService configuration
		hostname = conf.get("smhbase.discovery.hostname", "localhost");
		discoveryServiceLocation = conf.get("smhbase.discovery.location",
				"localhost");
		sleepTime = conf.getInt("smhbase.discovery.sleepTime", 200);
		incTime = conf.getInt("smhbase.discovery.incTime", 100);
		retries = conf.getInt("smhbase.discovery.retries", 5);


		databaseSchemaPath = conf.get("smhbase.schema");
		String file = getClass().getResource("/"+databaseSchemaPath).getFile();

		schema = new DatabaseSchema(file);
	}

	public Relay createRelay(MessageBroker broker) throws IOException {
		DiscoveryServiceConfiguration conf = new DiscoveryServiceConfiguration(
				discoveryServiceLocation, playerID, hostname, relayPort,
				sleepTime, incTime, retries);
		return new IORelay(relayHost, relayPort, broker, conf);
	}

	public int getPlayerID() {
		return playerID;
	}

	public String getPlayerIDasString() {
		return String.valueOf(playerID);
	}

	public int getBatchSize() {
		return batchSize;
	}

	public int getPreRandomSize() {
		return preRandomElems;
	}

	public String getRelayHost() {
		return relayHost;
	}

	public int getRelayPort() {
		return relayPort;
	}

	public boolean isDevelopment() {
		return isDevelopment;
	}

	public DatabaseSchema getSchema() {
		return schema;
	}
}
