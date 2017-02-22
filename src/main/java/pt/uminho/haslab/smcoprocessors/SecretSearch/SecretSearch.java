package pt.uminho.haslab.smcoprocessors.SecretSearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smcoprocessors.protocolresults.ResultsLengthMissmatch;
import pt.uminho.haslab.smhbase.interfaces.Player;

public class SecretSearch {

	private final RegionCoprocessorEnvironment env;

	private final Player player;

	private final SearchCondition searchValue;

	private final boolean stopOnMatch;

	private final Column col;

	public SecretSearch(SearchCondition searchValue,
			RegionCoprocessorEnvironment env, Player player,
			SmpcConfiguration config, boolean stopOnMatch, Column col) {

		this.searchValue = searchValue;
		this.env = env;
		this.player = player;
		this.stopOnMatch = stopOnMatch;
		this.col = col;
	}

	private void prepareScan(Scan scan) {
		// This implementation is currently only searching in a single column.
		scan.addColumn(col.getCf(), col.getCq());
	}

	public List<byte[]> search() throws IOException, ResultsLengthMissmatch {
		List<byte[]> rows = new ArrayList<byte[]>();

		boolean hasMore;
		Scan scan = new Scan();
		prepareScan(scan);
		RegionScanner scanner = env.getRegion().getScanner(scan);
		List<Cell> results = new ArrayList<Cell>();

		do {

			hasMore = scanner.next(results);
			/**
			 * Assuming that the scanner only returns on cell since it is only
			 * looking at a single column;
			 */
			Cell cell = results.get(0);

			byte[] row = CellUtil.cloneRow(cell);
			byte[] value = CellUtil.cloneValue(cell);

			SharemindPlayer splayer = (SharemindPlayer) player;
			boolean result = searchValue.evaluateCondition(value, row, splayer);
			rows.add(row);

			if (result && stopOnMatch) {
				hasMore = false;
			}
			results.clear();
		} while (hasMore);
		scanner.close();
		return rows;
	}

}
