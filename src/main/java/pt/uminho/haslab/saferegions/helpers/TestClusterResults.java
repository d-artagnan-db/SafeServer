package pt.uminho.haslab.saferegions.helpers;

import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;
import pt.uminho.haslab.testingutils.ClusterResults;

import java.math.BigInteger;
import java.util.List;

public class TestClusterResults extends ClusterResults {

	public TestClusterResults(List<Result> resp) {
		super(resp);
	}

	public BigInteger unshare(int nbits) {
		BigInteger u1 = new BigInteger(results.get(0).value());
		BigInteger u2 = new BigInteger(results.get(1).value());
		BigInteger u3 = new BigInteger(results.get(2).value());

		SharemindSharedSecret secret = new SharemindSharedSecret(nbits, u1, u2,
				u3);

		return secret.unshare();
	}

}
