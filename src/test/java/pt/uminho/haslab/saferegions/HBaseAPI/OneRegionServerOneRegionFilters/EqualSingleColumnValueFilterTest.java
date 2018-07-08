package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import pt.uminho.haslab.smpc.helpers.RandomGenerator;

public class EqualSingleColumnValueFilterTest extends AbsSingleColumnValueFilterTest {

    public EqualSingleColumnValueFilterTest() throws Exception {
        super();
        RandomGenerator.initBatch(31, 100);
    }


    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.EQUAL;
    }


}
