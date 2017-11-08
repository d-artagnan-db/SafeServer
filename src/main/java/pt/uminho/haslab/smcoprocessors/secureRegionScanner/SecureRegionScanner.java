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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SecureRegionScanner implements RegionScanner {
	static final Log LOG = LogFactory.getLog(SecureRegionScanner.class
			.getName());


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

		scan = new Scan(scanStartRow, scanStopRow);

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

		//return isFilterDone;
        return false;
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
		List<List<Cell>> localCells = new ArrayList<List<Cell>>();
		int counter = 0;

		do {
			List<Cell> localResults = new ArrayList<Cell>();
			hasMore = scanner.next(localResults);

			// Return case when there are no records in the table;
			if (!hasMore && localResults.isEmpty()) {
				return localCells;
			}

			localCells.add(localResults);
			counter += 1;
		} while (counter < batchSize && hasMore);

		return localCells;
	}

	public boolean next(List<Cell> results) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Next in SecureRegionScanner was issued ");
        }

		if (resultsCache.isBatchEmpty()) {
			List<List<Cell>> fRows = new ArrayList<List<Cell>>();

			do {
				List<List<Cell>> batch = loadBatch();

				if (!batch.isEmpty()) {
					// Only returns rows that satisfy the protocol
					fRows = this.handler.filterBatch(batch);
				}
			} while (hasMore && fRows.isEmpty());

			resultsCache.addListCells(fRows);
		}

        /**
         * After loading a batch, filtering the dataset and storing it on a cache, there are four possible scenarios.
         *
         *  The first scenario contemplates the final state of the execution of a scan when there are no more records to
         *  be read from the original scan or from the cache.  When this happens, the scanner must return an empty
         *  array as result and false to indicate the client that no more records can be read.
         *
         *  The second scenarios considers that are no more records tor read from the actual hbase scanner, but there
         *  are still some records on the cache.  In this case a record is read from the cache and if the cache becomes
         *  empty it returns the value from the cache and a false so the client does not make any more requests.
         *  If there are still records in the cache but the scan must stop after finding the first match, then send
         *  the first record of the cache and false for the client to stop.
         *  If a record is read from the cache but it still is not empty and the scan does not stop after the first
         *  record, it than returns true.
         *
         *  The third scenario considers when there are still more records to be read from the scan and the batch
         *  that was loaded did not have any record that validated a filter condition. In this case a new batch must
         *  be loaded and filtered. As such, it is returned an empty array and true for further requests. This case should
         *  not happen because records are loaded to cache until one is either found or there are no more records available.
         *
         *
         *
         *  The last case handles when there are still records to be read from the original scan and records still
         *  stored on the cache. In this case, a record is read from the cache and the result is true or false depending
         *  on if the scan should stop after finding a match. If more  records are to be read and the cache becomes empty,
         *  the next request will fill the cache.
         * */
        if (!hasMore && resultsCache.isBatchEmpty()) {
			results.addAll(new ArrayList<Cell>());
			return false;
        } else if (!hasMore && !resultsCache.isBatchEmpty()) {
            results.addAll(resultsCache.getNext());
            return !(handler.isStopOnMatch() || resultsCache.isBatchEmpty());
        } else if (hasMore && resultsCache.isBatchEmpty()) {
            results.addAll(new ArrayList<Cell>());
            return true;
        } else if (hasMore && !resultsCache.isBatchEmpty()) {
            results.addAll(resultsCache.getNext());
            return !handler.isStopOnMatch();
        } else {
            throw new IllegalStateException("Case not handled");
        }

    }

	public boolean next(List<Cell> result, int limit) throws IOException {
        LOG.error("Next with limit was issued");
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
            this.cells.addAll(cells);
		}

		public List<Cell> getNext() {
			return this.cells.poll();

		}

		boolean isBatchEmpty() {
			return cells.isEmpty();
		}

	}
}
