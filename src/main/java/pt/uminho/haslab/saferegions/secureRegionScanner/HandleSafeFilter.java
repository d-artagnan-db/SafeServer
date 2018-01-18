package pt.uminho.haslab.saferegions.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.SmpcConfiguration;
import pt.uminho.haslab.saferegions.secretSearch.*;
import pt.uminho.haslab.saferegions.secureFilters.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.safemapper.DatabaseSchema.isProtectedColumn;

public class HandleSafeFilter {

    private static final Log LOG = LogFactory.getLog(HandleSafeFilter.class
            .getName());
    private final TableSchema schema;
    private final Map<Column, List<SearchCondition>> safeFilters;
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

    private SmpcConfiguration config;


    public HandleSafeFilter(TableSchema schema, SmpcConfiguration config) {
        this.schema = schema;
        safeFilters = new HashMap<Column, List<SearchCondition>>();
        foundInvalidRecord = false;
        this.config = config;
    }

    public void processFilter(Filter inputFilter) {

        secureFilter = handleFilter(inputFilter);
        filterWasProcessed = true;
    }

    public List<List<Cell>> filterBatch(List<List<Cell>> rows, Map<Column, List<byte[]>> columnValues, List<byte[]> rowIDs, SharemindPlayer player) {
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


        // Get the protected column values and the row identifiers.
        LOG.debug("SafeFilters keys " + safeFilters.size());
        // Evaluate all of the SMPC protocols required for the filter.
        for (Column col : safeFilters.keySet()) {
            List<byte[]> values = columnValues.get(col);
            //LOG.debug("SafeFilter conditions size is " + safeFilters.get(col).size());
            for (SearchCondition safeFilter : safeFilters.get(col)) {
                // LOG.debug("Going to evaluate searchCondition " + safeFilter.getCondition());
                safeFilter.evaluateCondition(values, rowIDs, player);
                // LOG.debug("Condition evaluated");
            }

        }

        List<List<Cell>> result = filterDataset(rows);

        // Cleans the filters with SMPC indexes for next batch of cells
        secureFilter.reset();
        return result;

    }


    private List<List<Cell>> filterDataset(List<List<Cell>> rows) {
        List<List<Cell>> results = new ArrayList<List<Cell>>();

        for (List<Cell> row : rows) {
            boolean isValid = secureFilter.filterRow(row);
            if (isValid) {
                results.add(row);
            } else {
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
            case MUST_PASS_ALL:
                return SearchCondition.Condition.And;
            case MUST_PASS_ONE:
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
        String sFamily = new String(family);
        String sQualifier = new String(qualifier);

        CompareFilter.CompareOp operator = filter.getOperator();
        SearchCondition.Condition cond = getFilterCondition(operator);
        Column col = new Column(family, qualifier);

        boolean isProtectedColumn = isProtectedColumn(schema, family, qualifier);

        if (isProtectedColumn) {

            List<byte[]> values = new ArrayList<byte[]>();
            values.add(value);

            int nBits = getColumnFormatSize(family, qualifier);

            DatabaseSchema.CryptoType ctype = schema.getCryptoTypeFromQualifier(sFamily, sQualifier);
            SearchConditionFactory factory;
            String log;

            switch (ctype) {

                case ISMPC:
                    log = "IntSearchConditionFactory";
                    factory = new IntSearchConditionFactory(cond, nBits, values, config);
                    break;
                case LSMPC:
                    log = "LongSearchConditionFactory";
                    factory = new LongSearchConditionFactory(cond, nBits, values, config);
                    break;
                case SMPC:
                    log = "BigIntegerSearchConditionFactory";
                    factory = new BigIntegerSearchConditionFactory(cond, nBits, values, config);
                    break;
                default:
                    throw new IllegalStateException("CType not recognized");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(log);
            }
            SearchCondition searchCond = factory.conditionTransformer();

            //Initialize list of Search Conditions if this is the first one.
            if (!this.safeFilters.containsKey(col)) {
                this.safeFilters.put(col, new ArrayList<SearchCondition>());
            }

            this.safeFilters.get(col).add(searchCond);

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
            case LESS:
                return SearchCondition.Condition.Less;
            case LESS_OR_EQUAL:
                return SearchCondition.Condition.LessOrEqualThan;
            case EQUAL:
                return SearchCondition.Condition.Equal;
            case GREATER_OR_EQUAL:
                return SearchCondition.Condition.GreaterOrEqualThan;
            case GREATER:
                return SearchCondition.Condition.Greater;
            default:
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
