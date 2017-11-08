package pt.uminho.haslab.smcoprocessors.HBaseAPI.unprotected;

import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smcoprocessors.helpers.AbstractClusterTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractUnprotectedFilterTest extends AbstractClusterTest {

    public AbstractUnprotectedFilterTest() throws Exception {
        super();
    }

    protected void generateTableSchema() {
        String file = getClass().getResource("/unprotected-schema.xml").getFile();
        DatabaseSchema dSchema = new DatabaseSchema(file);
        this.schema = dSchema.getTableSchema("Teste");
        this.qualifierColTypes.put("User", new HashMap<String, ColType>());
        this.qualifierColTypes.get("User").put("Name", ColType.STRING);
        this.qualifierColTypes.get("User").put("Surname", ColType.STRING);
        this.qualifierColTypes.get("User").put("Age", ColType.INT);
    }

    protected boolean usesMPC(){ return false;}

    protected long getNumberOfRecords() {
        return 10;
    }

    protected int getNumberOfRegions() {
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
