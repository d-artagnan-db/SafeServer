package pt.uminho.haslab.saferegions.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.saferegions.secretSearch.SearchCondition;
import pt.uminho.haslab.saferegions.secretSearch.SharemindPlayer;
import pt.uminho.haslab.saferegions.secureFilters.*;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.safemapper.DatabaseSchema.isProtectedColumn;

public class HandleSafeFilter {

    private static final Log LOG = LogFactory.getLog(HandleSafeFilter.class
            .getName());
    private final Filter inputFilter;
    private final TableSchema schema;
    private final Map<Column, List<SearchCondition>> safeFilters;
    private final SharemindPlayer player;
    private boolean filterWasProcessed;

    /**
     * Stop On Match is used for WhileMatchFilters, it is turned to true to signal that this handler only needs to
     * iterate until it finds a record that is not valid.
     * <p>
     * When a match is found that is not valid than it sets the flag foundInvalidRecord to true.
     */
    private boolean stopOnInvalidRecord;
    private boolean foundInvalidRecord;

    private SecureFilter secureFilter;
    private boolean hasProtectedColumn;

    public HandleSafeFilter(TableSchema schema, Filter filter, Player player) {
        this.inputFilter = filter;
        this.schema = schema;
        safeFilters = new HashMap<Column, List<SearchCondition>>();
        this.player = (SharemindPlayer) player;
        foundInvalidRecord = false;
    }

    public void processFilter() {

        secureFilter = handleFilter(inputFilter);
        filterWasProcessed = true;
    }

    public List<List<Cell>> filterBatch(List<List<Cell>> rows) {
        /**
         * Clear flag foundInvalidRecord from previous execution.
         * This cleaning operation can not be on the end of the function because other objects (SecureRegionScanner)
         * might need the state from the previous execution. Only when  a new filter operation is requested can this be cleaned.
         */
        foundInvalidRecord = false;

        if (!filterWasProcessed) {
            throw new IllegalStateException(
                    "Filters must be processed before before filtering data");
        }
        Map<Column, List<byte[]>> columnValues = new HashMap<Column, List<byte[]>>();
        List<byte[]> rowIDs = new ArrayList<byte[]>();

        if(hasProtectedColumn){
            // Get the protected column values and the row identifiers.
            processDatasetValues(rows, columnValues, rowIDs);
            LOG.debug("SafeFilters keys " + safeFilters.size());
            // Evaluate all of the SMPC protocols required for the filter.
            for (Column col : safeFilters.keySet()) {
                List<byte[]> values = columnValues.get(col);
                LOG.debug("SafeFilter conditions size is " + safeFilters.get(col).size());
                for(SearchCondition safeFilter: safeFilters.get(col)){
                    LOG.debug("Going to evaluate searchCondition " + safeFilter.getCondition());
                    safeFilter.evaluateCondition(values, rowIDs, player);
                    LOG.debug("Condition evaluated");
                }

            }
        }

        List<List<Cell>> result = filterDataset(rows);

        // Cleans the filters with SMPC indexes for next batch of cells
        secureFilter.reset();
        return result;

    }

    private void processDatasetValues(List<List<Cell>> rows,
                                      Map<Column, List<byte[]>> columnValues, List<byte[]> rowIDs) {
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

    private List<List<Cell>> filterDataset(List<List<Cell>> rows) {
        List<List<Cell>> results = new ArrayList<List<Cell>>();

        for (List<Cell> row : rows) {
            boolean isValid = secureFilter.filterRow(row);
            if (isValid) {
                results.add(row);
            }else {
                foundInvalidRecord = true;
            }

            if (!isValid && stopOnInvalidRecord) {
                break;
            }
        }
        return results;
    }

    private SecureFilter handleFilter(Filter filter) {

        if (filter instanceof WhileMatchFilter) {
            return handleWhileMatchFilter((WhileMatchFilter) filter);
        } else if (filter instanceof FilterList) {
            return handleFilterList((FilterList) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilter((SingleColumnValueFilter) filter);
        } else if (filter instanceof RowFilter) {
            return handleRowFilter((RowFilter) filter);
        } else {
            throw new IllegalArgumentException("Filter " + filter
                    + " is not supported");
        }
    }

    private SecureFilter handleWhileMatchFilter(WhileMatchFilter filter) {
        Filter innerFilter = filter.getFilter();
        this.stopOnInvalidRecord = true;
        return handleFilter(innerFilter);
    }

    private SecureFilter handleFilterList(FilterList filter) {
        List<SecureFilter> filtersProcessed = new ArrayList<SecureFilter>();
        List<Filter> innerFilters = filter.getFilters();

        for (Filter f : innerFilters) {
            SecureFilter handledFilter = handleFilter(f);
            filtersProcessed.add(handledFilter);
        }

        SearchCondition.Condition cond = handleFilterOperator(filter
                .getOperator());

        return new SecureFilterList(cond, filtersProcessed);
    }

    private SearchCondition.Condition handleFilterOperator(
            FilterList.Operator operator) {

        switch (operator) {
            case MUST_PASS_ALL :
                return SearchCondition.Condition.And;
            case MUST_PASS_ONE :
                return SearchCondition.Condition.Or;
        }
        return null;
    }

    /**
     * This functions holds one of the most important parts of the class. It
     * checks if a filter processes a column protected with secret sharing or
     * not. If the filter is over a protected column, it creates an equivalent
     * SearchCondition that will process the MPC protocol and creates a
     * SecureFilter that works as a placeholder on the Filter tree. This
     * placeholder is then used when filtering a dataset as if it was a typical
     * HBase Filter.
     */
    private SecureFilter handleSingleColumnValueFilter(
            SingleColumnValueFilter filter) {
        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();

        CompareFilter.CompareOp operator = filter.getOperator();
        SearchCondition.Condition cond = getFilterCondition(operator);
        Column col = new Column(family, qualifier);

        boolean isProtectedColumn = isProtectedColumn(schema, family, qualifier);

        if (isProtectedColumn) {

            List<byte[]> values = new ArrayList<byte[]>();
            values.add(value);

            int nBits = getColumnFormatSize(family, qualifier);

            SearchCondition searchCond = AbstractSearchValue
                    .conditionTransformer(cond, nBits, values);

            //Initialize list of Search Conditions if this is the first one.
            if (!this.safeFilters.containsKey(col)) {
                this.safeFilters.put(col, new ArrayList<SearchCondition>());
            }

            this.safeFilters.get(col).add(searchCond);
            this.hasProtectedColumn = true;

            return new SearchConditionFilter(searchCond, col);

        } else {
            return new SecureSingleColumnValueFilter(col, cond,
                    filter.getComparator());
        }
    }

    private SecureFilter handleRowFilter(RowFilter filter) {
        CompareFilter.CompareOp operator = filter.getOperator();
        SearchCondition.Condition cond = getFilterCondition(operator);
        return new SecureIdentifierFilter(cond, filter.getComparator());
    }


    private int getColumnFormatSize(byte[] family, byte[] qualifier) {
        String sFamily = new String(family);
        String sQualifier = new String(qualifier);

        return schema.getFormatSizeFromQualifier(sFamily, sQualifier);
    }

    private SearchCondition.Condition getFilterCondition(
            CompareFilter.CompareOp operator) {

        switch (operator) {
            case LESS :
                return SearchCondition.Condition.Less;
            case LESS_OR_EQUAL :
                return SearchCondition.Condition.LessOrEqualThan;
            case EQUAL :
                return SearchCondition.Condition.Equal;
            case GREATER_OR_EQUAL :
                return SearchCondition.Condition.GreaterOrEqualThan;
            case GREATER :
                return SearchCondition.Condition.Greater;
            default :
                return null;
        }
    }

    public boolean isStopOnInvalidRecord() {
        return this.stopOnInvalidRecord;
    }

    public SecureFilter getSecureFilter() {
        return secureFilter;
    }

    public boolean foundInvalidRecord() {
        return foundInvalidRecord;
    }
}
