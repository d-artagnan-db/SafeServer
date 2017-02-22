package pt.uminho.haslab.smcoprocessors.helpers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.interfaces.SharedSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ClusterResults;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ClusterTables;

public class TestClusterTables extends ClusterTables {

	static final Log LOG = LogFactory.getLog(TestClusterTables.class.getName());

	public TestClusterTables(List<Configuration> configs, TableName tbname)
			throws IOException {
		super(configs, tbname);
	}

	/*
	 * Returns -1 if no match is found on the search. Otherwise it returns the
	 * row key
	 */
	public int equalEndpoint(int nbits, SharedSecret cmpValue, int requestID,
			SmpcConfiguration config) throws Throwable {
		LOG.debug("Entering equalEndpoint function " + nbits + " val "
				+ cmpValue.unshare() + " request " + requestID);

		int playerID = 1;
		SharemindSharedSecret ssecret = (SharemindSharedSecret) cmpValue;
		List<Get> gets = new ArrayList<Get>();

		Get getC1 = new Get(ssecret.getU1().toByteArray());
		Get getC2 = new Get(ssecret.getU2().toByteArray());
		Get getC3 = new Get(ssecret.getU3().toByteArray());
		gets.add(getC1);
		gets.add(getC2);
		gets.add(getC3);
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();
		for (Get get : gets) {
			get.setAttribute("targetPlayer", playerIDba);
			get.setAttribute("requestID", requestIDba);
		}

		ClusterResults results = this.get(gets);

		if (results.isInconsistant()) {
			throw new IllegalStateException(
					"One Result was empty but the others" + "were not");
		}

		if (results.allEmpty()) {
			return -1;
		} else {
			return Integer.parseInt(new String(results.getResult(0).getRow()));
		}
	}

	protected void scanWithStartAndStopRow(Dealer dealer, List<Scan> scans,
			byte[] startRow, byte[] stopRow) throws InvalidSecretValue {
		SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
				.share(new BigInteger(startRow));
		SharemindSharedSecret endSharedSecret = (SharemindSharedSecret) dealer
				.share(new BigInteger(stopRow));

		Scan firstScan = new Scan(startSharedSecret.getU1().toByteArray(),
				endSharedSecret.getU1().toByteArray());
		Scan secondScan = new Scan(startSharedSecret.getU2().toByteArray(),
				endSharedSecret.getU2().toByteArray());
		Scan thirdScan = new Scan(startSharedSecret.getU3().toByteArray(),
				endSharedSecret.getU3().toByteArray());

		scans.add(firstScan);
		scans.add(secondScan);
		scans.add(thirdScan);
	}

	protected void scanWithStartRow(Dealer dealer, List<Scan> scans,
			byte[] startRow) throws InvalidSecretValue {
		SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
				.share(new BigInteger(startRow));

		Scan firstScan = new Scan();
		firstScan.setStartRow(startSharedSecret.getU1().toByteArray());
		Scan secondScan = new Scan();
		secondScan.setStartRow(startSharedSecret.getU2().toByteArray());

		Scan thirdScan = new Scan();
		thirdScan.setStartRow(startSharedSecret.getU3().toByteArray());

		scans.add(firstScan);
		scans.add(secondScan);
		scans.add(thirdScan);
	}

	public void scanWithStopRow(Dealer dealer, List<Scan> scans, byte[] stopRow)
			throws InvalidSecretValue {
		SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
				.share(new BigInteger(stopRow));

		Scan firstScan = new Scan();
		firstScan.setStopRow(startSharedSecret.getU1().toByteArray());
		Scan secondScan = new Scan();
		secondScan.setStopRow(startSharedSecret.getU2().toByteArray());
		Scan thirdScan = new Scan();
		thirdScan.setStopRow(startSharedSecret.getU3().toByteArray());

		scans.add(firstScan);
		scans.add(secondScan);
		scans.add(thirdScan);
	}

	private List<Scan> getScans(Dealer dealer, byte[] startRow, byte[] stopRow)
			throws InvalidSecretValue {
		List<Scan> scans = new ArrayList<Scan>();
		LOG.debug("0-Start row are " + new BigInteger(startRow) + " / "
				+ new BigInteger(stopRow));

		if (startRow != null && stopRow != null) {
			scanWithStartAndStopRow(dealer, scans, startRow, stopRow);
		} else if (startRow != null && stopRow == null) {
			scanWithStartRow(dealer, scans, startRow);
		} else if (startRow == null && stopRow != null) {
			scanWithStopRow(dealer, scans, stopRow);
		} else if (startRow == null && stopRow == null) {
			LOG.debug("Going to do a full table scan");
			scans.add(new Scan());
			scans.add(new Scan());
			scans.add(new Scan());
		}

		return scans;
	}

	public List<Result> scanEndpoint(int nBits, byte[] startRow,
			byte[] stopRow, int requestID, SmpcConfiguration config,
			Dealer dealer) throws IOException, InterruptedException,
			InvalidSecretValue {

		int playerID = 1;
		LOG.debug("Start row are " + new BigInteger(startRow) + " / "
				+ new BigInteger(stopRow));
		List<Scan> scans = getScans(dealer, startRow, stopRow);

		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();

		for (Scan scan : scans) {
			scan.setAttribute("targetPlayer", playerIDba);
			scan.setAttribute("requestID", requestIDba);
		}

		ClusterScanResult results = this.scan(scans);

		if (!results.isConsistant()) {
			String error = "One Result was empty but the others were not";
			LOG.debug(error);
			throw new IllegalStateException(error);
		}

		if (!results.notEmpty()) {
			LOG.debug("Results are empty");
			return null;
		}

		results.setColumnFamily(config.getSecretFamily());
		results.setColumnQualifier(config.getSecretQualifier());
		results.setNbits(config.getnBits());

		List<Result> dResults = results.getDecodedResults();

		return dResults;

	}

}
