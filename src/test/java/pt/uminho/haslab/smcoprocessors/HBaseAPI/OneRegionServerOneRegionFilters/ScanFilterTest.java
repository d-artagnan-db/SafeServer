package pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smcoprocessors.helpers.AbstractClusterTest;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ScanFilterTest extends AbstractClusterTest {
    private Random randomGenerator;
    byte[] val1;
    byte[] val2;

    public ScanFilterTest() throws Exception {
        super();
        randomGenerator = new Random();


    }
    protected void generateTableSchema() {
        String file = getClass().getResource("/protected-schema.xml").getFile();
        DatabaseSchema dSchema = new DatabaseSchema(file);
        this.schema = dSchema.getTableSchema("Teste");
        this.qualifierColTypes.put("User", new HashMap<String, ColType>());
        this.qualifierColTypes.get("User").put("Name", ColType.STRING);
        this.qualifierColTypes.get("User").put("Surname", ColType.STRING);
        this.qualifierColTypes.get("User").put("Age", ColType.INT);

    }

    protected long getNumberOfRecords() {
        return 15;
    }

    protected Filter getFilterOnUnprotectedColumn() {
        String cf = "User";
        String cq = "Age_original";
        String cqOrig = "Age";

        List<byte[]> values = this.generatedValues.get(cf).get(cqOrig);

        int index1 = randomGenerator.nextInt(values.size());
        int index2 = randomGenerator.nextInt(values.size());


        LOG.debug("Chosen indexes were  "+ index1 + ":"+index2) ;
        val1 = values.get(index1);
        val2 = values.get(index2);
        BigInteger bVal1 = new BigInteger(val1);
        BigInteger bVal2 = new BigInteger(val2);

        int cmpRes = bVal2.compareTo(bVal1);
        if(cmpRes < 0 ){
            byte[] aux = val1;
            val1 = val2;
            val2 = aux;
        }

        LOG.debug("Chosen values were "+  new BigInteger(val1)+":"+ new BigInteger(val2));

        Filter gte =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, val1);
        Filter lt =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.LESS, val2);
        FilterList list  = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(gte);
        list.addFilter(lt);

        return list;
    }

    protected Filter getFilterOnProtectedColumn() {
        String cf = "User";
        String cq = "Age";

        Filter gte =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, val1);
        Filter lt =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.LESS, val2);
        FilterList list  = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(gte);
        list.addFilter(lt);
        return list;
    }

    protected int getNumberOfRegions() {
        return 1;
    }

    protected int getNumberOfRegionsServers() {
        return 1;
    }

    protected List<String> getResources() {
        List<String> resources = new ArrayList<String>();

        for(int i=0; i < 3; i++){
            String resource = "hbase-site-"+i+".xml";
            resources.add(resource);
        }

        return resources;
    }

    protected boolean usesMPC() {
        return true;
    }
}
