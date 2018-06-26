package pt.uminho.haslab.saferegions.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.List;
import java.util.Random;

public class OrFilterListTest extends
        AbstractUnprotectedFilterTest {

    private Random randomGenerator;

    public OrFilterListTest() throws Exception {
        super();
        randomGenerator = new Random();

    }

    protected Filter getFilterOnUnprotectedColumn() {
        String cf = "User";
        String cqName = "Name";
        String cqAge = "Age";
        List<byte[]> values = this.generatedValues.get(cf).get(cqName);

        int indexChosen = randomGenerator.nextInt(values.size());
        LOG.debug("Index chosen was " + indexChosen);
        byte[] val = values.get(indexChosen);
        int ageIndex = randomGenerator.nextInt(values.size());

        LOG.debug("Random value chosen  was " + new String(val));
        byte[] ageVal = this.generatedValues.get(cf).get(cqAge).get(ageIndex);
        Filter nameFilter = new SingleColumnValueFilter(cf.getBytes(), cqName.getBytes(), CompareFilter.CompareOp.EQUAL, val);
        Filter ageFilter = new SingleColumnValueFilter(cf.getBytes(), cqAge.getBytes(), CompareFilter.CompareOp.EQUAL, ageVal);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(nameFilter);
        filterList.addFilter(ageFilter);
        return filterList;
    }

    protected Filter getFilterOnProtectedColumn() {
        return getFilterOnUnprotectedColumn();
    }

}
