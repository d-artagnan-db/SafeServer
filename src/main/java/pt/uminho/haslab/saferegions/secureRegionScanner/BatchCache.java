package pt.uminho.haslab.saferegions.secureRegionScanner;

import org.apache.hadoop.hbase.Cell;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BatchCache {

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
