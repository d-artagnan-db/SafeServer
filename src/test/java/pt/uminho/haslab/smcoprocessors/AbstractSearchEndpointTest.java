package pt.uminho.haslab.smcoprocessors;

import java.util.List;
import org.junit.After;
import org.junit.Test;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.smcoprocessors.helpers.Clusters;
import pt.uminho.haslab.smcoprocessors.helpers.TestClusterTables;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ValuesGenerator;

public abstract class AbstractSearchEndpointTest {

	static final Log LOG = LogFactory.getLog(AbstractSearchEndpointTest.class
			.getName());

	private final Clusters clusters;
	protected final SmpcConfiguration config;

	public AbstractSearchEndpointTest() throws Exception {

		List<String> resources = new ArrayList<String>();

		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");

		}

		clusters = new Clusters(resources);
		Thread.sleep(30000);

		String resource = "hbase-site-0.xml";

		Configuration conf = new Configuration();
		conf.addResource(resource);
		config = new SmpcConfiguration(conf);
	}

	@After
	public void tearDown() throws IOException, InterruptedException {
		System.out.println("TearDown invocked");
		clusters.tearDown();
		// Wait for ports to be free for next tests
		Thread.sleep(1000);
	}

	public abstract void searchEndpointComparision(Dealer dealer,
			List<BigInteger> values, TestClusterTables tables, int nbits)
			throws InvalidNumberOfBits, InvalidSecretValue, Throwable;

	protected abstract String getTestTableName();

	@Test
	public void search() throws IOException, InterruptedException,
			InvalidNumberOfBits, InvalidSecretValue, Throwable {

		/*
		 * The values generated by these testes must be between 0 and 62. The
		 * reasons for this are that the sharemind dealer allways uses a mod of
		 * nbits +1.If the generated value is 62 then the mod will be 63. Since
		 * the values are being exchnaged by a protocol buffer message that is
		 * assuming longs, and the secrets are converted to longs, then the
		 * maximum value of a long is (2^63) -1. Long uses 64 bits, one for sign
		 * and the other 63 to hold the value.
		 */
		int nbits = ValuesGenerator.maxBits;

		List<BigInteger> values = ValuesGenerator.equalSearchEndpointList();

		TestClusterTables tables = clusters.createTables(getTestTableName(),
				new String(config.getSecretFamily()));
		Dealer dealer = new SharemindDealer(nbits);

		byte[] cf = config.getSecretFamily();
		byte[] cq = config.getSecretQualifier();

		/**
		 * Store a set of random generated values minus the last element. The
		 * last element is used to test a get of a value that is not stored.
		 * 
		 */
		for (int i = 0; i < values.size(); i++) {

			BigInteger value = values.get(i);
			SharemindSharedSecret secret = (SharemindSharedSecret) dealer
					.share(value);

			byte[] id = ("" + i).getBytes();

			Put putC1 = new Put(id);
			putC1.add(cf, cq, secret.getU1().toByteArray());
			putC1.add(cf, "val".getBytes(), value.toByteArray());

			Put putC2 = new Put(id);
			putC2.add(cf, cq, secret.getU2().toByteArray());
			putC2.add(cf, "val".getBytes(), value.toByteArray());

			Put putC3 = new Put(id);
			putC3.add(cf, cq, secret.getU3().toByteArray());
			putC3.add(cf, "val".getBytes(), value.toByteArray());

			LOG.debug(i + "- ( " + secret.getU1() + ", " + secret.getU2()
					+ ", " + secret.getU3() + ")");
			tables.put(0, putC1).put(1, putC2).put(2, putC3);

		}
		searchEndpointComparision(dealer, values, tables, nbits);
	}
}
