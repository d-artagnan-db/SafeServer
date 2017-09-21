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
	public int equalEndpoint(SharedSecret cmpValue, int requestID,
			String secretFamily, String secretQualifier) throws Throwable {
		LOG.debug(" Val " + cmpValue.unshare() + " request " + requestID);

		int playerID = 1;
		SharemindSharedSecret sSecret = (SharemindSharedSecret) cmpValue;
		List<Get> gets = new ArrayList<Get>();

		Get getC1 = new Get(sSecret.getU1().toByteArray());
		Get getC2 = new Get(sSecret.getU2().toByteArray());
		Get getC3 = new Get(sSecret.getU3().toByteArray());
		gets.add(getC1);
		gets.add(getC2);
		gets.add(getC3);
		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();
		for (Get get : gets) {
			get.setAttribute(OperationAttributesIdentifiers.TargetPlayer,
					playerIDba);
			get.setAttribute(OperationAttributesIdentifiers.RequestIdentifier,
					requestIDba);
			get.setAttribute(OperationAttributesIdentifiers.ProtectedColumn,
					"true".getBytes());
			get.setAttribute(OperationAttributesIdentifiers.SecretFamily,
					secretFamily.getBytes());
			get.setAttribute(OperationAttributesIdentifiers.SecretQualifier,
					secretQualifier.getBytes());
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
		LOG.debug("0-Start row are " + startRow + " / " + stopRow);

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

	public List<Result> scanWithFilter(byte[] startRow, byte[] stopRow,
                                       int requestID, SmpcConfiguration config, Dealer dealer,
                                       String secretFamily, String secretQualifier) throws IOException, InterruptedException, InvalidSecretValue {

        int playerID = 1;

        byte[] requestIDba = ("" + requestID).getBytes();
        byte[] playerIDba = ("" + playerID).getBytes();

        SharemindSharedSecret startSharedSecret = (SharemindSharedSecret) dealer
                .share(new BigInteger(startRow));

        List<Scan> scans = new ArrayList<Scan>();
        for(int i=0; i < 3; i++){
            Scan  scan = new Scan();
            scan.setFilter(new SingleColumnValueFilter(secretFamily.getBytes(), secretQualifier.getBytes(), EQUAL, startRow));
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
            scans.add(scan);
        }
        scans.get(0).setAttribute(OperationAttributesIdentifiers.FilterValue, startSharedSecret.getU1().toByteArray());
        scans.get(1).setAttribute(OperationAttributesIdentifiers.FilterValue, startSharedSecret.getU2().toByteArray());
        scans.get(2).setAttribute(OperationAttributesIdentifiers.FilterValue, startSharedSecret.getU3().toByteArray());

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

	public List<Result> scanEndpoint(byte[] startRow, byte[] stopRow,
			int requestID, SmpcConfiguration config, Dealer dealer,
			String secretFamily, String secretQualifier) throws IOException,
			InterruptedException, InvalidSecretValue {

		int playerID = 1;
		LOG.debug("Start row are " + Arrays.toString(startRow) + " / "
				+ Arrays.toString(stopRow));
		List<Scan> scans = getScans(dealer, startRow, stopRow);

		byte[] requestIDba = ("" + requestID).getBytes();
		byte[] playerIDba = ("" + playerID).getBytes();

		for (Scan scan : scans) {
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
		}

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
