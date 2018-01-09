package pt.uminho.haslab.saferegions.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smpc.interfaces.Player;

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

	    if(LOG.isDebugEnabled()){
	        LOG.debug("getRegionInfo");
        }
        return env.getRegion().getRegionInfo();
	}

	public boolean isFilterDone() throws IOException {
	    if (LOG.isDebugEnabled()){
	        LOG.debug("IsFilterDone issued");
	    }
        return handler.isStopOnInvalidRecord() && handler.isStopOnInvalidRecord();
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("loaded batch size of " + counter + " but was asked to load at most " + batchSize);
		}

		return localCells;
	}

	public boolean next(List<Cell> results) throws IOException {

	    if(LOG.isDebugEnabled()) {
            LOG.debug("Next in SecureRegionScanner was issued ");
        }

        boolean run = resultsCache.isBatchEmpty() && !(handler.isStopOnInvalidRecord() && handler.foundInvalidRecord());

        if (run) {
            List<List<Cell>> fRows = new ArrayList<List<Cell>>();

			do {
				List<List<Cell>> batch = loadBatch();
				LOG.debug("Batch loaded");
				if (!batch.isEmpty()) {
					LOG.debug("Going to filterBatch");
					// Only returns rows that satisfy the protocol
					fRows = this.handler.filterBatch(batch);
				}
			} while (hasMore && fRows.isEmpty());

			resultsCache.addListCells(fRows);
		}

        /**
         * After loading a batch, filtering the dataset and storing it on a cache, there are four possible scenarios.
         * The data loaded to the cache are only the the rows that have already been filtered by the HandleSafeFilter
         * which returns all valid records or only the records valid until a match is found that is not valid. The
         * second case only happens when a WhileMatchFilter is requested.
         *
         *  The first scenario contemplates the final state of the execution of a scan when there are no more records to
         *  be read from the original scan or from the cache.  When this happens, the scanner must return an empty
         *  array as result and false to indicate the client that no more records can be read.
         *
         *  The second scenarios considers that are no more records to be read from the actual hbase scanner, but there
         *  are still some records on the cache.  In this case a record is read from the cache and if the cache becomes
         *  empty it returns the value from the cache and false is returned to the client so the client does not make any
         *  more requests.

         *  If a record is read from the cache but it still is not empty and the scan does not stop after the first
         *  record, it than returns true.
         *
         *  The third scenario considers when there are still more records to be read from the scan and the cache is empty
         *  either because every record was already taken from the cache or the batch loaded did not have any record
         *  that matched. There are two possible options for this case, if the scan must stop after find a record that
         *  does not match  than it checks the handler to see if it must stop and if it did find a record that did not
         *  match. In this case it returns false. If the scan does not stop when a record that does not match a condition,
         *  it returns true to load a new batch and keep on filtering.
         *
         *
         *  The last case handles when there are still records to be read from the original scan and records still
         *  stored on the cache. If more  records are to be read and the cache becomes empty,
         *  the next request will fill the cache.
         * */

        if (!hasMore && resultsCache.isBatchEmpty()) {
			results.addAll(new ArrayList<Cell>());
			return false;
        } else if (!hasMore && !resultsCache.isBatchEmpty()) {
            results.addAll(resultsCache.getNext());
			return !resultsCache.isBatchEmpty();
        } else if (hasMore && resultsCache.isBatchEmpty()) {
            results.addAll(new ArrayList<Cell>());
			return !(handler.isStopOnInvalidRecord() && handler.foundInvalidRecord());
        } else if (hasMore && !resultsCache.isBatchEmpty()) {
            results.addAll(resultsCache.getNext());
			return true;
        } else {
            throw new IllegalStateException("Case not handled");
        }
    }

    public void close() throws IOException {
	    LOG.debug("Close issued");
		((SharemindPlayer) player).cleanValues();
		scanner.close();
	}

    public boolean next(List<Cell> result, int limit) throws IOException {
        LOG.error("Next with limit was issued");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean reseek(byte[] row) throws IOException {
		LOG.error("reseek was issued");
		throw new UnsupportedOperationException("Not supported yet.");
    }

    public long getMaxResultSize()
    {
        if(LOG.isDebugEnabled()){
            LOG.debug("getMaxResultSize issued");
        }
	    return scanner.getMaxResultSize();
	}

    public long getMvccReadPoint() {
	    if(LOG.isDebugEnabled()){
            LOG.debug("getMvccReadPoint issued");
	    }
	    return scanner.getMvccReadPoint();
    }

    public boolean nextRaw(List<Cell> result) throws IOException {
	    if(LOG.isDebugEnabled()){
	        LOG.debug("NextRaw issued");
	    }
		return this.next(result);
    }

    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
	    if(LOG.isDebugEnabled()){
		    LOG.error("Next raw with limit was issued");
	    }
		throw new UnsupportedOperationException("Not supported yet.");
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
