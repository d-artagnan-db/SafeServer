package pt.uminho.haslab.smcoprocessors.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smcoprocessors.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SecureRegionScanner implements RegionScanner {
	static final Log LOG = LogFactory.getLog(SecureRegionScanner.class
			.getName());

	static final Log TIMES = LogFactory.getLog("protoLatency");

	private final RegionCoprocessorEnvironment env;

	private final Player player;

	private final HandleSafeFilter handler;
	private final Scan scan;

	private final RegionScanner scanner;
	private final BatchCache resultsCache;
	private final Batcher batcher;

	private boolean isFilterDone;
	private boolean hasMore;

	public SecureRegionScanner(RegionCoprocessorEnvironment env, Player player,
			SmpcConfiguration config, HandleSafeFilter handler,
			byte[] scanStartRow, byte[] scanStopRow) throws IOException {
		this.handler = handler;
		this.env = env;
		this.player = player;

		//scan = new Scan(scanStartRow, scanStopRow);
        scan = new Scan();
		LOG.debug(player.getPlayerID() + " region name is "
				+ env.getRegion().getRegionNameAsString() + " with state "
				+ env.getRegion().isAvailable() + " - "
				+ env.getRegion().isClosed() + " - "
				+ env.getRegion().isClosing());

		scanner = env.getRegion().getScanner(scan);
		batcher = new Batcher(config);
		resultsCache = new BatchCache();
		hasMore = false;
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

	private List<List<Cell>> loadBatch() throws IOException {
		int batchSize = batcher.batchSize();
		LOG.debug("BatchSize " + batchSize);
		List<List<Cell>> localCells = new ArrayList<List<Cell>>();
		int counter = 0;

		LOG.debug("Going to load cells");
		do {
			List<Cell> localResults = new ArrayList<Cell>();
			hasMore = scanner.next(localResults);

			// Return case when there are no records in the table;
			//LOG.debug("hasMore? "+hasMore);
			//LOG.debug("localResults empty? "+ localResults.isEmpty());
			if (hasMore == false && localResults.isEmpty()) {
				LOG.debug("Going to return new empty BatchGetResult");
				return localCells;
			}

			localCells.add(localResults);
			counter += 1;
			//LOG.debug("Counter is "+counter);
			//LOG.debug("hasMore? "+hasMore);
		} while (counter < batchSize && hasMore);
		LOG.debug("Cells loaded");
		return localCells;
	}

	public boolean next(List<Cell> results) throws IOException {
		LOG.debug("Next in SecureRegionScanner was issued ");

		LOG.debug("ResultsCache empty? " + resultsCache.isBatchEmpty());
		if (resultsCache.isBatchEmpty()) {
			List<List<Cell>> fRows = new ArrayList<List<Cell>>();

			do {
				List<List<Cell>> batch = loadBatch();

				LOG.debug("bRes empty? " + batch.isEmpty());

				if (!batch.isEmpty()) {
					// Only returns rows that satisfy the protocol
                    LOG.debug("Filter scanned rows");
					fRows = this.handler.filterBatch(batch);
				}
				LOG.debug("hasMore? " + hasMore + " fRows " + fRows.isEmpty());
			} while (hasMore && fRows.isEmpty());

			LOG.debug("Going to add  cells");
			resultsCache.addListCells(fRows);
		}

		LOG.debug("hasMore? " + hasMore + " resultsCache? "
				+ resultsCache.isBatchEmpty());
		if (!hasMore && resultsCache.isBatchEmpty()) {
			results.addAll(new ArrayList<Cell>());
			isFilterDone = true;
			return false;
		}

		LOG.debug("StopOnMatch " + handler.isStopOnMatch());

		results.addAll(resultsCache.getNext());
		boolean res = !(resultsCache.isBatchEmpty() || handler.isStopOnMatch());
		isFilterDone = !res;
		return res;
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

	private class BatchCache {

		private Queue<List<Cell>> cells;

		BatchCache() {
			cells = new LinkedList<List<Cell>>();
		}

		void addListCells(List<List<Cell>> cells) {
			for (List<Cell> lCells : cells) {
				this.cells.add(lCells);
			}
		}

		public List<Cell> getNext() {
			return this.cells.poll();

		}

		boolean isBatchEmpty() {
			return cells.isEmpty();
		}

	}
}
