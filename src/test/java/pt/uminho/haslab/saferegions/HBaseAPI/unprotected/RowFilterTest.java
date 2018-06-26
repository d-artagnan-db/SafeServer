package pt.uminho.haslab.saferegions.HBaseAPI.unprotected;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.util.List;
import java.util.Random;

public class RowFilterTest extends AbstractUnprotectedFilterTest {

    private Random randomGenerator;


    public RowFilterTest() throws Exception {
        super();
        randomGenerator = new Random();

    }

    protected Filter getFilterOnUnprotectedColumn() {
        String cf = "User";
        String cq = "Name";
        List<byte[]> values = this.generatedValues.get(cf).get(cq);

        int indexChosen = randomGenerator.nextInt(values.size());
        LOG.debug("Index chosen was " + indexChosen);

        return new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(("" + indexChosen).getBytes()));
    }

    protected Filter getFilterOnProtectedColumn() {
        return getFilterOnUnprotectedColumn();
    }

}
