package pt.uminho.haslab.saferegions.benchmarks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.comunication.MessageBroker;
import pt.uminho.haslab.saferegions.comunication.Relay;
import pt.uminho.haslab.saferegions.comunication.SharemindMessageBroker;
import pt.uminho.haslab.saferegions.helpers.FilePaths;
import pt.uminho.haslab.saferegions.helpers.RedisUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

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
        /**
         *
         * Hadoop configuration seems to have a weird behavior regarding file names.
         * It can find file given their names without the path as long as theses files are built in the jar. If the
         * files are not in the jar built but passed in the classpath, then it can not find the files.
         *
         * If a given a full file path, than it can't find the file either.
         *
         * the only way I found that works is to get the full file path and convert it to an URL.
         *
         * The FilePaths.getPath(String) function does the work of getting a full file path loaded to the classpath given
         * a file name.
         *
         * */
        System.out.println("Going to get resource " + resource);

        URL f = new File(FilePaths.getPath(resource)).toURI().toURL();
        //System.out.println("Going to get url " +f);
        conf.addResource(f);
        conf.reloadConfiguration();
        searchConf = new SmpcConfiguration(conf);
        if (playerID == 0) {
            RedisUtils.flushAll(searchConf.getDiscoveryServiceLocation());
        }
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
