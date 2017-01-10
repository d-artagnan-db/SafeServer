package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smhbase.interfaces.Player;

public class SecureRegionScanner implements RegionScanner {

	private final RegionCoprocessorEnvironment env;

	private final Player player;

	private final SearchCondition searchValue;

	private final boolean stopOnMatch;

	private final Column col;

	private boolean isFilterDone;
	private boolean hasMore;

	private final Scan scan;
	private final RegionScanner scanner;

	public SecureRegionScanner(SearchCondition searchValue,
			RegionCoprocessorEnvironment env, Player player,
			SmpcConfiguration config, boolean stopOnMatch, Column col)
			throws IOException {
		System.out.println("Secure Region Scanner was created");
		this.searchValue = searchValue;
		this.env = env;
		this.player = player;
		this.stopOnMatch = stopOnMatch;
		this.col = col;
		scan = new Scan();
		scanner = env.getRegion().getScanner(scan);

	}

	public HRegionInfo getRegionInfo() {
		return env.getRegionInfo();
	}

	public boolean isFilterDone() throws IOException {
		System.out.println("is filter done " + isFilterDone);

		return isFilterDone;
	}

	public boolean reseek(byte[] row) throws IOException {
		System.out.println("reseek was issued");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public long getMaxResultSize() {
		System.out.println("Mvcc result size was issued");
		return scanner.getMaxResultSize();
	}

	public long getMvccReadPoint() {
		System.out.println("MvccReadPoint was issued");
		return scanner.getMvccReadPoint();
	}

	public boolean nextRaw(List<Cell> result) throws IOException {
		System.out.println("Next raw was issued");
		return this.next(result);
		// throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		System.out.println("Next raw with limit was issued");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean next(List<Cell> results) throws IOException {
		System.out.println("Next in SecureRegionScanner was issued "
				+ results.size());
		boolean matchFound = false;
		List<Cell> localResults = new ArrayList<Cell>();

		// Search for a matching encrypted value.

		do {
			localResults.clear();
			hasMore = scanner.next(localResults);
			byte[] rowID = null;
			byte[] protectedValue = null;
			for (Cell cell : localResults) {

				if (Arrays.equals(CellUtil.cloneFamily(cell), col.getCf())
						&& Arrays.equals(CellUtil.cloneQualifier(cell),
								col.getCq())) {
					rowID = CellUtil.cloneRow(cell);
					protectedValue = CellUtil.cloneValue(cell);
				}

			}

			SharemindPlayer splayer = (SharemindPlayer) player;
			// System.out.println("Before Searching for match. HasMore: "
			// + hasMore + "; MatchFound: " + matchFound);
			System.out.println("Going to do search for rowID "
					+ new String(rowID) + " with the protectedValue "
					+ new BigInteger(protectedValue));
			matchFound = searchValue.evaluateCondition(protectedValue, rowID,
					splayer);

			System.out.println("After Search for match. HasMore: " + hasMore
					+ "; MatchFound: " + matchFound);

		} while (hasMore & !matchFound);

		// Copy the resulting cells if a match was found.
		System.out.println("Going to copy Results of rowID "
				+ new String(CellUtil.cloneRow(localResults.get(0))));
		if (matchFound) {
			results.addAll(localResults);
		}

		localResults.clear();
		if (matchFound & stopOnMatch) {
			hasMore = false;
		}
		System.out.println("Next result is " + hasMore);

		return hasMore;
	}

	public boolean next(List<Cell> result, int limit) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void close() throws IOException {
		scanner.close();
	}

}
