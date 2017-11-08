package pt.uminho.haslab.smcoprocessors.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smcoprocessors.helpers.AbstractClusterTest;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class WhileMatchFilterTest extends AbstractClusterTest{

    private Random randomGenerator;
    byte[] chosenVal;

    public WhileMatchFilterTest() throws Exception {
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

        int indexChosen = randomGenerator.nextInt(values.size());

        LOG.debug("Index chosen was "+ indexChosen);
        chosenVal = values.get(indexChosen);
        LOG.debug("Random value chosen  was "+  new BigInteger(chosenVal));

        Filter f = new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, chosenVal);
        return new WhileMatchFilter(f);
    }

    protected Filter getFilterOnProtectedColumn() {
        String cf = "User";
        String cq = "Age";
        Filter f =  new SingleColumnValueFilter(cf.getBytes(), cq.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, chosenVal);
        return new WhileMatchFilter(f);
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
