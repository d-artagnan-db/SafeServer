package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.smcoprocessors.secretSearch.AbstractSearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.ComposedSearchValue;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.And;
import static pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition.Condition.Or;

/**
 * This class is used to process a Scan with a filter  over a table column protected with secret Sharing.
 * The idea of this class is to receive an original scan issued by the client and divide the filter in two parts,
 * the part of the filter that works over columns not protected with secret sharing and filters that do work with columns
 * protected with secret sharing.
 * <p>
 * This class is later used by the SecretSearch to do a correct scan over the stored values.
 */
public class ScanFilterHandler {

    private final Filter inputFilter;

    private Filter unprotectedFilter;

    private SearchCondition protectedFilter;

    private boolean filterWasProcessed;

    private boolean stopOnMatch;

    private final int targetPlayer;
    private final int nBits;

    public ScanFilterHandler(Filter filter, int targetPlayer, int nBits) {
        this.inputFilter = filter;
        this.targetPlayer = targetPlayer;
        this.nBits = nBits;
    }

    public void processFilter() {

        unprotectedFilter = handleUnprotectedFilter(inputFilter);
        protectedFilter = handleProtectedFilter(inputFilter);
        filterWasProcessed = true;
    }

    private SearchCondition handleProtectedFilter(Filter filter) {
        if (filter instanceof WhileMatchFilter) {
            return handleWhileMatchFilterSC((WhileMatchFilter) filter);
        } else if (filter instanceof FilterList) {
            return handleFilterListSC((FilterList) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilterSC((SingleColumnValueFilter) filter);
        } else {
            throw new IllegalArgumentException("Filter " + filter + " is not supported");
        }
    }

    /**
     * Currently we are assuming that if there is one WhileMatchFilter than all of the Scan will
     * stop when a match is found. It is assumed that this is only True when dealing with requests from the DQE
     * that always have a WhileMatchFilter as the outermost filter.
     * This can also be used to simulate a get when the table identifiers are protected with secret sharing. In this
     * case it is assumed the first match is the correct result.
     */
    private SearchCondition handleWhileMatchFilterSC(WhileMatchFilter filter) {
        Filter nestedFilter = filter.getFilter();
        stopOnMatch = true;
        return handleProtectedFilter(nestedFilter);
    }

    /**
     * This function will work with a list that can have more than a SearchCondition.
     * [SearchCondition, SearchCondition, SearchCondition]
     * Its objective is to group all of this SearchConditions in a single one by creating a tree of search conditions
     * with the composed search condition.
     * ComposedSearchCondition(ComposedSearchCondition(SearchCondition, SearchCondition, cond), SearchCondition, cond)
     * <p>
     * To achive this goal, there has to be an additional function that loops the array and joins the conditions.
     * <p>
     * middle Step -> [ComposedSearchCondition(SearchCondition, SearchCondition, cond), SearchCondition]
     * last Step => [ComposedSearchCondition(ComposedSearchCondition(SearchCondition, SearchCondition, cond), SearchCondition, cond)]
     * <p>
     * The cond variable corresponds to the condition inside the filterList
     */
    private SearchCondition handleFilterListSC(FilterList filterList) {
        List<Filter> filters = filterList.getFilters();
        List<SearchCondition> conditions = new ArrayList<SearchCondition>();

        for (Filter f : filters) {
            conditions.add(handleProtectedFilter(f));
        }
        SearchCondition.Condition cond;

        if (filterList.getOperator() == MUST_PASS_ALL) {
            cond = And;
        } else {
            cond = Or;
        }
        return joinSearchConditions(filterNulls(conditions), cond);

    }

    private List<SearchCondition> filterNulls(List<SearchCondition> conditions) {
        List<SearchCondition> resList = new ArrayList<SearchCondition>();

        for (SearchCondition f : conditions) {
            if (f != null) {
                resList.add(f);
            }
        }
        return resList;
    }

    private SearchCondition joinSearchConditions(List<SearchCondition> conditions, SearchCondition.Condition cond) {
        List<SearchCondition> resList = new ArrayList<SearchCondition>();
        if (conditions.size() >= 2) {
            //Concatenate the first two values in a ComposedSearchValue
            resList.add(new ComposedSearchValue(cond, conditions.get(0), conditions.get(1), targetPlayer));
            //Add the remaining elements of the list to the resultList
            resList.addAll(conditions.subList(2, conditions.size()));

            /**
             * Call the same function again to enter a recursive call and join the remaining values of the list until
             * there is only 1 left.
             * */
            return joinSearchConditions(resList, cond);
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            throw new IllegalStateException("List size is invalid. Size is " + conditions.size());
        }
    }

    private SearchCondition.Condition getFilterCondition(CompareFilter.CompareOp operator) {

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

    private SearchCondition handleSingleColumnValueFilterSC(SingleColumnValueFilter filter) {
        CompareFilter.CompareOp operator = filter.getOperator();
        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();

        boolean isProtectedColumn = isProtectedColumn(family, qualifier);

        if (isProtectedColumn) {
            SearchCondition.Condition cond = getFilterCondition(operator);
            List<byte[]> values = new ArrayList<byte[]>();
            values.add(value);
            return AbstractSearchValue.conditionTransformer(cond, nBits, values, targetPlayer);
        } else {
            return null;
        }

    }

    private Filter handleUnprotectedFilter(Filter filter) {
        if (filter instanceof WhileMatchFilter) {
            return handleWhileMatchFilter((WhileMatchFilter) filter);
        } else if (filter instanceof FilterList) {
            return handleFilterList((FilterList) filter);
        } else if (filter instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilter((SingleColumnValueFilter) filter);
        } else {
            throw new IllegalArgumentException("Filter " + filter + " is not supported");
        }
    }

    private Filter handleWhileMatchFilter(WhileMatchFilter filter) {
        Filter nestedFilter = filter.getFilter();
        Filter nf = handleUnprotectedFilter(nestedFilter);
        if (nf != null) {
            return new WhileMatchFilter(nf);
        } else {
            return null;
        }
    }

    private Filter handleFilterList(FilterList filter) {

        List<Filter> filtersProcessed = new ArrayList<Filter>();
        List<Filter> innerFilters = filter.getFilters();

        for (Filter f : innerFilters) {
            Filter handledFilter = handleUnprotectedFilter(f);
            if (handledFilter != null) {
                filtersProcessed.add(handledFilter);
            }
        }
        return new FilterList(filter.getOperator(), filtersProcessed);
    }

    private Filter handleSingleColumnValueFilter(SingleColumnValueFilter filter) {
        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();

        boolean isProtectedColumn = isProtectedColumn(family, qualifier);

        if (isProtectedColumn) {
            return null;
        } else {
            return filter;
        }
    }

    private boolean isProtectedColumn(byte[] family, byte[] qualifier) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Filter getUnprotectedFilter() {
        if (!filterWasProcessed) {
            throw new IllegalStateException("The processFilter function must be issued before accessing the unprotectedFilter");
        }
        return unprotectedFilter;
    }

    public SearchCondition getProtectedFilter() {
        if (!filterWasProcessed) {
            throw new IllegalStateException("The processFilter function must be issued before accessing the SearchCondition");
        }
        return protectedFilter;
    }

    public boolean isStopOnMatch() {
        if (!filterWasProcessed) {
            throw new IllegalStateException("The processFilter function was not issued previously");
        }
        return stopOnMatch;
    }
}
