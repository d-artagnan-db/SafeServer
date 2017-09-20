package pt.uminho.haslab.smcoprocessors.benchmarks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smcoprocessors.comunication.MessageBroker;
import pt.uminho.haslab.smcoprocessors.comunication.Relay;
import pt.uminho.haslab.smcoprocessors.comunication.SharemindMessageBroker;

import java.io.IOException;

public abstract class TestRegionServer extends Thread implements RegionServer {

	private static final Log LOG = LogFactory.getLog(TestRegionServer.class
			.getName());
	protected final Relay relay;
	protected final MessageBroker broker;
	protected final SmpcConfiguration searchConf;
	protected final int playerID;
	protected boolean runStatus;

	public TestRegionServer(int playerID) throws IOException {
		String resource = "hbase-site-" + playerID + ".xml";

		Configuration conf = new Configuration();
		conf.addResource(resource);
		searchConf = new SmpcConfiguration(conf);

		broker = new SharemindMessageBroker();

		relay = searchConf.createRelay(broker);

		this.playerID = playerID;
		runStatus = true;

	}

	public abstract void doComputation();

	@Override
	public void run() {

		try {
			// Start and wait for other players.
			relay.bootRelay();

			broker.waitRelayStart();
			doComputation();

			relay.stopRelay();
			Thread.sleep(1000);

		} catch (InterruptedException ex) {
			runStatus = false;
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		} catch (IOException ex) {
			runStatus = false;
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		}

	}

	public void stopRegionServer() throws IOException, InterruptedException {
		this.join();
	}

	public void startRegionServer() {
		this.start();
	}

	public boolean getRunStatus() {
		return this.runStatus;
	}
}
