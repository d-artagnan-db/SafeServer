package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	static final Log LOG = LogFactory.getLog(SecureRegionScanner.class
			.getName());

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
		LOG.debug("is filter done " + isFilterDone);

		return isFilterDone;
	}

	public boolean reseek(byte[] row) throws IOException {
		LOG.debug("reseek was issued");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public long getMaxResultSize() {
		LOG.debug("Mvcc result size was issued");
		return scanner.getMaxResultSize();
	}

	public long getMvccReadPoint() {
		LOG.debug("MvccReadPoint was issued");
		return scanner.getMvccReadPoint();
	}

	public boolean nextRaw(List<Cell> result) throws IOException {
		LOG.debug("Next raw was issued");
		return this.next(result);
	}

	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		LOG.debug("Next raw with limit was issued");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public boolean next(List<Cell> results) throws IOException {
		LOG.debug("Next in SecureRegionScanner was issued ");
		boolean matchFound = false;
		List<Cell> localResults = new ArrayList<Cell>();

		// Search for a matching encrypted value.

		do {
			localResults.clear();
			hasMore = scanner.next(localResults);
			// If there is nothing to search;
			if (hasMore == false && localResults.isEmpty()) {
				results.addAll(localResults);
				return false;
			}

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

			matchFound = searchValue.evaluateCondition(protectedValue, rowID,
					splayer);

		} while (hasMore & !matchFound);

		if (matchFound) {
			results.addAll(localResults);
		}

		localResults.clear();

		if (matchFound & stopOnMatch) {
			hasMore = false;
		}

		return hasMore;
	}

	public boolean next(List<Cell> result, int limit) throws IOException {
		LOG.debug("Next with limit was issued");
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void close() throws IOException {
		LOG.debug("close was issued");

		((SharemindPlayer) player).cleanValues();
		scanner.close();
	}

}
