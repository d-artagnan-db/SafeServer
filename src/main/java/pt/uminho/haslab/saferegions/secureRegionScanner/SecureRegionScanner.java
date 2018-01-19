package pt.uminho.haslab.saferegions.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smpc.interfaces.Player;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static pt.uminho.haslab.safemapper.DatabaseSchema.isProtectedColumn;

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

	private final TableSchema schema;
	private final SmpcConfiguration config;

	private final BigInteger regionIdent;

    private static final Lock cacheDataLock = new ReentrantLock();
    private static final Map<BigInteger, BatchData> mapBatchCachedData  = new HashMap<BigInteger, BatchData>();;




	public SecureRegionScanner(RegionCoprocessorEnvironment env, Player player,
                               SmpcConfiguration config,
                               byte[] scanStartRow, byte[] scanStopRow, byte[] regionStartRow, byte[] regionStopRow, TableSchema schema, Scan originalScan) throws IOException {

	    this.env = env;
		this.player = player;
		this.config = config;
		batcher = new Batcher(config);

        scan = new Scan(scanStartRow, scanStopRow);

		scanner = env.getRegion().getScanner(scan);

		resultsCache = new BatchCache();
		hasMore = false;

        this.schema = schema;

        handler = new HandleSafeFilter(schema, config);
        handler.processFilter(originalScan.getFilter());


		if(regionStartRow.length == 0){
		    this.regionIdent = BigInteger.ZERO;
        }else{
            this.regionIdent = new BigInteger(regionStartRow);

        }


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
			    if(config.isCachedData() && mapBatchCachedData.containsKey(regionIdent)){
			        BatchData cacheBatchData = mapBatchCachedData.get(regionIdent);
                    List<List<Cell>> batch = cacheBatchData.getRows();
                    fRows = this.handler.filterBatch(batch, cacheBatchData.getColumnValues(), cacheBatchData.getRowIDs(),  (SharemindPlayer) player);

                }else{
                    List<List<Cell>> batch = loadBatch();
                    //LOG.debug("Batch loaded");
                    if (!batch.isEmpty()) {
                        //LOG.debug("Going to filterBatch");
                        // Only returns rows that satisfy the protocol
                        BatchData batchData = new BatchData(batch);
                        batchData.processDatasetValues();
                        fRows = this.handler.filterBatch(batch, batchData.getColumnValues(), batchData.getRowIDs(),  (SharemindPlayer) player);
                        /**
                         *  If in this part of the code and cache data is requested than its one of the threads that
                         *  is doing the first load of data.
                         *  There maybe more threads reading for the first time the data.
                         */
                        if(config.isCachedData()){
                            cacheDataLock.lock();

                            if(!mapBatchCachedData.containsKey(regionIdent)){
                                mapBatchCachedData.put(regionIdent, batchData);
                            }
                            cacheDataLock.unlock();

                        }
				    }
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


    private class BatchData{
        private final Map<Column, List<byte[]>> columnValues;
        private final List<byte[]> rowIDs;
        private final List<List<Cell>> rows;

        public BatchData(List<List<Cell>> rows){
            this.rows = rows;
            this.columnValues  = new HashMap<Column, List<byte[]>>();
            this.rowIDs = new ArrayList<byte[]>();

        }

        public void processDatasetValues() {

                // Process rows and store the values of the protected columns in a Map.
                for (List<Cell> row : rows) {

                    for (Cell cell : row) {
                        byte[] cellCF = CellUtil.cloneFamily(cell);
                        byte[] cellCQ = CellUtil.cloneQualifier(cell);
                        byte[] rowID = CellUtil.cloneRow(cell);
                        byte[] cellVal = CellUtil.cloneValue(cell);

                        if (isProtectedColumn(schema, cellCF, cellCQ)) {
                            Column col = new Column(cellCF, cellCQ);

                            if (!columnValues.containsKey(col)) {
                                columnValues.put(col, new ArrayList<byte[]>());
                            }
                            columnValues.get(col).add(cellVal);
                            rowIDs.add(rowID);

                        }
                    }
                }
            }

        public Map<Column, List<byte[]>> getColumnValues(){
	        return this.columnValues;

        }

        public List<byte[]> getRowIDs(){
            return this.rowIDs;
        }

        public List<List<Cell>> getRows() {
            return rows;
        }
    }


}
