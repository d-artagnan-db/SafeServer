package pt.uminho.haslab.saferegions.HBaseAPI.OneRegionServerOneRegionFilters;

import org.apache.hadoop.hbase.filter.CompareFilter;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smpc.helpers.RandomGenerator;

import java.util.HashMap;

public class IntGreaterOrEqualThanTest extends AbsSingleColumnValueFilterTest {

    public IntGreaterOrEqualThanTest() throws Exception {
        super();
        RandomGenerator.initIntBatch(100);
    }


    protected void generateTableSchema() {
        String file = getClass().getResource("/int-protected-schema.xml").getFile();
        DatabaseSchema dSchema = new DatabaseSchema(file);
        this.schema = dSchema.getTableSchema("Teste");
        this.qualifierColTypes.put("User", new HashMap<String, ColType>());
        this.qualifierColTypes.get("User").put("Name", ColType.STRING);
        this.qualifierColTypes.get("User").put("Surname", ColType.STRING);
        this.qualifierColTypes.get("User").put("Age", ColType.INTEGER);
        this.qualifierColTypes.get("User").put("Cost", ColType.INTEGER);
        this.qualifierColTypes.get("User").put("Stuff", ColType.LONG);
    }

    protected long getNumberOfRecords() {
        return 10;
    }

    @Override
    CompareFilter.CompareOp getComparator() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}
