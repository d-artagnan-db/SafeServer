package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.smcoprocessors.OperationAttributesIdentifiers;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.interfaces.SharedSecret;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ClusterResults;
import pt.uminho.haslab.testingutils.ClusterScanResult;
import pt.uminho.haslab.testingutils.ClusterTables;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;

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
	public BigInteger equalScanEndpoint(SharedSecret cmpValue, int requestID,
			String secretFamily, String secretQualifier,
			SmpcConfiguration config) throws Throwable {
		LOG.debug(" Val " + cmpValue.unshare() + " request " + requestID);

		int playerID = 1;
		SharemindSharedSecret sSecret = (SharemindSharedSecret) cmpValue;
		List<BigInteger> value = new ArrayList<BigInteger>();
		value.add(sSecret.getU1());
		value.add(sSecret.getU2());
		value.add(sSecret.getU3());
		List<Scan> scans = new ArrayList<Scan>();

		Scan scanC1 = new Scan();
		Scan scanC2 = new Scan();
		Scan scanC3 = new Scan();

		scans.add(scanC1);
		scans.add(scanC2);
		scans.add(scanC3);
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();

		for (int i = 0; i < scans.size(); i++) {
			Scan scan = scans.get(i);
			scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer,
					playerIDba);
			scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,
					requestIDba);
			scan.setAttribute(OperationAttributesIdentifiers.ProtectedColumn,
					"true".getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.SecretFamily,
					secretFamily.getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.SecretQualifier,
					secretQualifier.getBytes());
			scan.setAttribute(OperationAttributesIdentifiers.ScanForEqualVal,
					value.get(i).toByteArray());
		}
		ClusterScanResult results = this.scan(scans);
		results.setColumnFamily(secretFamily.getBytes());
		results.setColumnQualifier(secretQualifier.getBytes());
		results.setNbits(config.getnBits());
		List<Result> decResults = results.getDecodedResults();
		LOG.debug("Results size is " + decResults);
		if (!results.isConsistant()) {
			throw new IllegalStateException(
					"One Result was empty but the others" + "were not");
		}

		if (decResults.isEmpty()) {
			return BigInteger.valueOf(-1);
		} else {
			LOG.debug("DecResults is " + decResults.get(0));
			LOG.debug("DecResults  BigInteger is "
					+ new BigInteger(decResults.get(0).getRow()));

			return new BigInteger(decResults.get(0).getRow());
		}
	}

	protected void scanSetRow(Dealer dealer, List<Scan> scans, byte[] row,
			String attribute) throws InvalidSecretValue {
		if (row != null) {
			SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
					.share(new BigInteger(row));
			List<BigInteger> secrets = new ArrayList<BigInteger>();
			secrets.add(startSharedSecret.getU1());
			secrets.add(startSharedSecret.getU2());
			secrets.add(startSharedSecret.getU3());

			for (int i = 0; i < scans.size(); i++) {
				Scan scan = scans.get(i);
				scan.setAttribute(attribute, secrets.get(i).toByteArray());

			}
		}
	}

	private List<Scan> getScans() throws InvalidSecretValue {
		List<Scan> scans = new ArrayList<Scan>();
		scans.add(new Scan());
		scans.add(new Scan());
		scans.add(new Scan());
		return scans;
	}

	private void setScanAttributes(List<Scan> scans, byte[] playerID,
			byte[] requestID, byte[] protectedColumn, byte[] secretFamily,
			byte[] secretQualifier) {
		for (Scan scan : scans) {
			scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer,
					playerID);
			scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,
					requestID);
			scan.setAttribute(OperationAttributesIdentifiers.ProtectedColumn,
					protectedColumn);
			scan.setAttribute(OperationAttributesIdentifiers.SecretFamily,
					secretFamily);
			scan.setAttribute(OperationAttributesIdentifiers.SecretQualifier,
					secretQualifier);
		}
	}

	public List<Result> scanEndpoint(byte[] startRow, byte[] stopRow,
			int requestID, SmpcConfiguration config, Dealer dealer,
			String secretFamily, String secretQualifier) throws IOException,
			InterruptedException, InvalidSecretValue {

		int playerID = 1;
		LOG.debug("Start row are " + Arrays.toString(startRow) + " / "
				+ Arrays.toString(stopRow));

		List<Scan> scans = getScans();
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();
		setScanAttributes(scans, playerIDba, requestIDba, "true".getBytes(),
				secretFamily.getBytes(), secretQualifier.getBytes());
		scanSetRow(dealer, scans, startRow,
				OperationAttributesIdentifiers.ScanStartVal);
		scanSetRow(dealer, scans, stopRow,
				OperationAttributesIdentifiers.ScanStopVal);
		ClusterScanResult results = this.scan(scans);

		if (!results.isConsistant()) {
			String error = "One Result was empty but the others were not";
			LOG.debug(error);
			throw new IllegalStateException(error);
		}

		if (!results.notEmpty()) {
			LOG.debug("Results are empty");
			return new ArrayList<Result>();
		}

		results.setColumnFamily(secretFamily.getBytes());
		results.setColumnQualifier(secretQualifier.getBytes());
		results.setNbits(config.getnBits());
		return results.getDecodedResults();
	}

}
