package pt.uminho.haslab.smcoprocessors;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.Test;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchCondition;
import pt.uminho.haslab.smcoprocessors.secretSearch.SearchValue;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class ScanFilterHandlerTest {

    private final byte[] column = "teste".getBytes();
    private final byte[] val = BigInteger.ONE.toByteArray();

    @Test
    public void testeGreaterThanSCVF(){

        CompareFilter.CompareOp cop = CompareFilter.CompareOp.GREATER_OR_EQUAL;
        SingleColumnValueFilter filter = new SingleColumnValueFilter(column, column, cop, val);
        ScanFilterHandler sfh = new ScanFilterHandler(filter, 1, 1);
        sfh.processFilter();
        assertEquals(null, filter);
        assertEquals(true, sfh.getProtectedFilter() instanceof SearchValue);
        SearchValue sv = (SearchValue) sfh.getProtectedFilter();
        assertEquals(SearchCondition.Condition.GreaterOrEqualThan, sv.getCondition());


    }
}
