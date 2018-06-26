package pt.uminho.haslab.saferegions.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AndFilterListTest extends AbstractUnprotectedFilterTest {

    private Random randomGenerator;


    public AndFilterListTest() throws Exception {
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
        LOG.debug("Random value chosen  was " + new String(val));
        byte[] ageVal = this.generatedValues.get(cf).get(cqAge).get(indexChosen);
        Filter nameFilter = new SingleColumnValueFilter(cf.getBytes(), cqName.getBytes(), CompareFilter.CompareOp.EQUAL, val);
        Filter ageFilter = new SingleColumnValueFilter(cf.getBytes(), cqAge.getBytes(), CompareFilter.CompareOp.EQUAL, ageVal);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(nameFilter);
        filterList.addFilter(ageFilter);
        return filterList;
    }

    protected Filter getFilterOnProtectedColumn() {
        return getFilterOnProtectedColumn();
    }

    protected long getNumberOfRecords() {
        return 15;
    }

    protected int getNumberOfClusters() {
        return 1;
    }

    protected int getNumberOfRegionsServers() {
        return 1;
    }

    protected List<String> getResources() {
        List<String> resources = new ArrayList<String>();
        String resource = "unprotected-site.xml";
        resources.add(resource);
        return resources;
    }
}
