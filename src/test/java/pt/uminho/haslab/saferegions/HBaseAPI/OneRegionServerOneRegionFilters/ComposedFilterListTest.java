package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.saferegions.helpers.AbstractClusterTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ComposedFilterListTest extends AbstractClusterTest {

    private Random randomGenerator;
    byte[] age;
    byte[] stuff;

    public ComposedFilterListTest() throws Exception {
        super();
        randomGenerator = new Random();


    }
    protected void generateTableSchema() {
        String file = getClass().getResource("/int-protected-schema.xml").getFile();
        DatabaseSchema dSchema = new DatabaseSchema(file);
        this.schema = dSchema.getTableSchema("Teste");
        this.qualifierColTypes.put("User", new HashMap<String, AbstractClusterTest.ColType>());
        this.qualifierColTypes.get("User").put("Name", AbstractClusterTest.ColType.STRING);
        this.qualifierColTypes.get("User").put("Surname", AbstractClusterTest.ColType.STRING);
        this.qualifierColTypes.get("User").put("Age", ColType.INTEGER);
        this.qualifierColTypes.get("User").put("Cost", ColType.INTEGER);
        this.qualifierColTypes.get("User").put("Stuff", ColType.LONG);
    }

    protected long getNumberOfRecords() {
        return 1000;
    }

    protected Filter getFilterOnUnprotectedColumn() {
        String cf = "User";
        String cq = "Age_original";
        String cqOrig = "Age";

        String cqS = "Stuff_original";
        String cqSorig = "Stuff";

        List<byte[]> ageValues = this.generatedValues.get(cf).get(cqOrig);
        List<byte[]> stuffValues = this.generatedValues.get(cf).get(cqSorig);

        int index1 = randomGenerator.nextInt(ageValues.size());
        int index2 = randomGenerator.nextInt(stuffValues.size());


        LOG.debug("Chosen indexes were  "+ index1 + ":"+index2) ;
        age = ageValues.get(index1);
        stuff = stuffValues.get(index2);


        Filter gte =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, age);
        Filter lt =  new SingleColumnValueFilter(cf.getBytes(), cqS.getBytes(), CompareFilter.CompareOp.LESS, stuff);
        FilterList list  = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        list.addFilter(gte);
        list.addFilter(lt);

        return list;
    }

    protected Filter getFilterOnProtectedColumn() {
        String cf = "User";
        String cqAge = "Age";
        String cqStuff = "Stuff";


        Filter gte =  new SingleColumnValueFilter(cf.getBytes(), cqAge.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, age);
        Filter lt =  new SingleColumnValueFilter(cf.getBytes(), cqStuff.getBytes(), CompareFilter.CompareOp.LESS, stuff);
        FilterList list  = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        list.addFilter(gte);
        list.addFilter(lt);
        return list;
    }

    protected int getNumberOfRegions() {
        return 4;
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
