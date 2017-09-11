package pt.uminho.haslab.smcoprocessors.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import pt.uminho.haslab.smcoprocessors.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.SmpcConfiguration;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.io.IOException;
import java.util.*;

public class SecureRegionScanner implements RegionScanner {
    static final Log LOG = LogFactory.getLog(SecureRegionScanner.class
            .getName());

    static final Log TIMES = LogFactory.getLog("protoLatency");

    private final RegionCoprocessorEnvironment env;

    private final Player player;

    private final SearchCondition searchValue;

    private final boolean stopOnMatch;

    private final Column col;

    private boolean isFilterDone;

    private boolean hasMore;

    private final Scan scan;
    private final RegionScanner scanner;

    private final BatchCache resultsCache;

    private final Batcher batcher;

    private class BatchCache {

        private Queue<List<Cell>> cells;

        public BatchCache() {
            cells = new LinkedList<List<Cell>>();
        }

        public void addListCells(List<List<Cell>> cells) {
            for (List<Cell> lCells : cells) {
                this.cells.add(lCells);
            }
        }

        public void addCells(List<Cell> cells) {
            this.cells.add(cells);
        }

        public List<Cell> getNext() {
            return this.cells.poll();

        }

        public boolean isBatchEmpty() {
            return cells.isEmpty();
        }

    }

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

    private class BatchGetResult {
        private List<byte[]> rowIDS;
        private List<byte[]> protectedValues;
        private List<List<Cell>> localCells;

        public BatchGetResult() {
            rowIDS = new ArrayList<byte[]>();
            protectedValues = new ArrayList<byte[]>();
            localCells = new ArrayList<List<Cell>>();
        }

        public BatchGetResult(List<byte[]> rowIDS,
                              List<byte[]> protectedValues, List<List<Cell>> localCells) {
            this.rowIDS = rowIDS;
            this.protectedValues = protectedValues;
            this.localCells = localCells;
        }

        public List<byte[]> getRowIDS() {
            return rowIDS;
        }

        public void setRowIDS(List<byte[]> rowIDS) {
            this.rowIDS = rowIDS;
        }

        public List<byte[]> getProtectedValues() {
            return protectedValues;
        }

        public void setProtectedValues(List<byte[]> protectedValues) {
            this.protectedValues = protectedValues;
        }

        public List<List<Cell>> getLocalCells() {
            return localCells;
        }

        public void setLocalCells(List<List<Cell>> localCells) {
            this.localCells = localCells;
        }

        public boolean isEmpty() {
            return rowIDS.isEmpty();
        }
    }

    private BatchGetResult loadBatch() throws IOException {
        int batchSize = batcher.batchSize();
        LOG.debug("BatchSize " + batchSize);
        List<byte[]> rowIDS = new ArrayList<byte[]>();
        List<byte[]> protectedValues = new ArrayList<byte[]>();
        List<List<Cell>> localCells = new ArrayList<List<Cell>>();
        int counter = 0;

        LOG.debug("Going to load cells");
        do {
            List<Cell> localResults = new ArrayList<Cell>();
            hasMore = scanner.next(localResults);

            // Return case when there are no records in the table;
            // LOG.debug("hasMore? "+hasMore);
            // LOG.debug("localResults empty? "+ localResults.isEmpty());
            if (hasMore == false && localResults.isEmpty()) {
                // LOG.debug("Going to return new empty BatchGetResult");
                return new BatchGetResult();
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
            /*
             * LOG.debug(player.getPlayerID() + " found row with id " + new
			 * String(rowID) + " -> " + new BigInteger(protectedValue));
			 */
            rowIDS.add(rowID);
            protectedValues.add(protectedValue);
            localCells.add(localResults);
            counter += 1;
            // LOG.debug("Counter is "+counter);
            // LOG.debug("hasMore? "+hasMore);
        } while (counter < batchSize && hasMore);
        LOG.debug("Cells loaded");
        return new BatchGetResult(rowIDS, protectedValues, localCells);
    }

    private List<List<Cell>> searchBatch(BatchGetResult bRes) {
        SharemindPlayer splayer = (SharemindPlayer) player;

        List<byte[]> procVals = bRes.getProtectedValues();
        List<byte[]> rowIDS = bRes.getRowIDS();
        List<List<Cell>> localCells = bRes.getLocalCells();
        List<List<Cell>> filtredCells = new ArrayList<List<Cell>>();

        LOG.debug("Going to evaluate condition");
        long start = System.nanoTime();
        List<Boolean> condResults = searchValue.evaluateCondition(procVals,
                rowIDS, splayer);

        long stop = System.nanoTime();
        long elapsed = stop - start;
        TIMES.info(player.getPlayerID() + ", " + searchValue.getCondition()
                + ", " + elapsed);
        LOG.debug("Condition evaluated in " + player.getPlayerID() + ", "
                + searchValue.getCondition() + ", " + elapsed);

        LOG.debug("Results size " + condResults.size());
        LOG.debug("rowIDS size " + rowIDS.size());
        if (condResults.size() != rowIDS.size()) {
            throw new IllegalStateException();
        }
        LOG.debug("Going to filter results");
        for (int i = 0; i < condResults.size(); i++) {
            /*
             * LOG.debug("Loop index " + i + " of " + condResults.size());
			 * LOG.debug(player.getPlayerID() + " ID is " + new
			 * String(rowIDS.get(i)) + " and has result " + condResults.get(i));
			 */
            if (condResults.get(i).equals(Boolean.TRUE)) {
                filtredCells.add(localCells.get(i));
            }
        }
        LOG.debug("Filtered Cells size is " + filtredCells.size());
        return filtredCells;
    }

    public boolean next(List<Cell> results) throws IOException {
        LOG.debug("Next in SecureRegionScanner was issued ");

        LOG.debug("ResultsCach empty? " + resultsCache.isBatchEmpty());
        if (resultsCache.isBatchEmpty()) {
            List<List<Cell>> fRows = new ArrayList<List<Cell>>();

            do {
                BatchGetResult bRes = loadBatch();

                LOG.debug("bRes empty? " + bRes.isEmpty());

                if (!bRes.isEmpty()) {
                    // Only returns rows that satisfy the protocol
                    fRows = searchBatch(bRes);
                }
                LOG.debug("hasMore? " + hasMore + " fRows " + fRows.isEmpty());
            } while (hasMore && fRows.isEmpty());

            LOG.debug("Going to add  cells");
            resultsCache.addListCells(fRows);

        }

        LOG.debug("hasMore? " + hasMore + " resultsCache? "
                + resultsCache.isBatchEmpty());
        if (hasMore == false && resultsCache.isBatchEmpty()) {
            results.addAll(new ArrayList<Cell>());
            return false;
        }

        LOG.debug("StopOnMatch " + stopOnMatch);

        results.addAll(resultsCache.getNext());
        return !(resultsCache.isBatchEmpty() || this.stopOnMatch);

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
