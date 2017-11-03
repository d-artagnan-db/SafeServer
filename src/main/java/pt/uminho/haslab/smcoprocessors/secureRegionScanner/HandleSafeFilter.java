package pt.uminho.haslab.smcoprocessors.secureRegionScanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smcoprocessors.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.secretSearch.SharemindPlayer;
import pt.uminho.haslab.smcoprocessors.secureFilters.*;
import pt.uminho.haslab.smhbase.interfaces.Player;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HandleSafeFilter {


    /**
     * Tests whether a column is protected with secret sharing or not. If a
     * column is protected with another technique it must return false for now.
     */
    public static boolean isProtectedColumn(TableSchema schema, byte[] family, byte[] qualifier) {
        String sFamily = new String(family);
        String sQualifier = new String(qualifier);

        DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(
                sFamily, sQualifier);
        return type.equals(DatabaseSchema.CryptoType.SMPC);
    }

	private static final Log LOG = LogFactory.getLog(HandleSafeFilter.class
			.getName());
	private final Filter inputFilter;
	private final TableSchema schema;
	private final Map<Column, SearchCondition> safeFilters;
	private final SharemindPlayer player;
	private boolean filterWasProcessed;
	private boolean stopOnMatch;
	private SecureFilter secureFilter;

	private boolean hasProtectedColumn;

	public HandleSafeFilter(TableSchema schema, Filter filter, Player player) {
		this.inputFilter = filter;
		this.schema = schema;
		safeFilters = new HashMap<Column, SearchCondition>();
		this.player = (SharemindPlayer) player;
	}

	public void processFilter() {

		secureFilter = handleFilter(inputFilter);
		filterWasProcessed = true;
	}

	public List<List<Cell>> filterBatch(List<List<Cell>> rows) {

		if (!filterWasProcessed) {
			throw new IllegalStateException(
					"Filters must be processed before before filtering data");
		}
		Map<Column, List<byte[]>> columnValues = new HashMap<Column, List<byte[]>>();
		List<byte[]> rowIDs = new ArrayList<byte[]>();

		if(hasProtectedColumn){
		    LOG.debug("Store protected columns in SearchCondition");
		    // Get the protected column values and the row identifiers.
		    processDatasetValues(rows, columnValues, rowIDs);

            // Evaluate all of the SMPC protocols required for the filter.
            for (Column col : columnValues.keySet()) {
                List<byte[]> values = columnValues.get(col);
                safeFilters.get(col).evaluateCondition(values, rowIDs, player);

            }
        }

        LOG.debug("Filter dataset");
        List<List<Cell>> result = filterDataset(rows);

        LOG.debug("Reset filters for next batch");
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
				    //LOG.debug("Found protected column");
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
           LOG.debug("Filter row " + row + " with filter "+ row);
			boolean isValid = secureFilter.filterRow(row);
            LOG.debug("Row is valid " +  isValid);
			if (isValid) {
			    LOG.debug("Add row to result");
				results.add(row);
			}

			if (stopOnMatch) {
				break;
			}
		}
        LOG.debug("Return resulting rows");
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
		this.stopOnMatch = true;
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
			this.safeFilters.put(col, searchCond);
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

		LOG.debug("Family is " + family);
		LOG.debug("sQualifier is " + qualifier);

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

	public boolean isStopOnMatch() {
		return this.stopOnMatch;
	}

	public SecureFilter getSecureFilter() {
		return secureFilter;
	}
}
