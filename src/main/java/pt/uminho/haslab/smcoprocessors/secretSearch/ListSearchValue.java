package pt.uminho.haslab.smcoprocessors.secretSearch;

import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.smcoprocessors.secureRegionScanner.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class not currently used but it can be used in the future to support multiple
 * conditions of search if required.
 */
public class ListSearchValue {

    private final Map<Column, List<SearchValue>> searchValues;

    public ListSearchValue(Column col, List<SearchValue> searchValues) {
        // Sort it by column
        this.searchValues = new HashMap<Column, List<SearchValue>>();

        for (SearchValue val : searchValues) {

            if (!this.searchValues.containsKey(col)) {
                this.searchValues.put(col, new ArrayList<SearchValue>());
            }
            this.searchValues.get(col).add(val);

        }
    }

    private List<Column> getUniqueColumns() {
        List<Column> columns = new ArrayList<Column>();

        for (Column col : searchValues.keySet()) {
            columns.add(col);
        }

        return columns;
    }

    public void prepareScan(Scan scan) {

        List<Column> columns = getUniqueColumns();

        for (Column col : columns) {
            scan.addColumn(col.getCf(), col.getCq());
        }
    }

    public List<SearchValue> getConditions(Column col) {
        return searchValues.get(col);
    }

}
