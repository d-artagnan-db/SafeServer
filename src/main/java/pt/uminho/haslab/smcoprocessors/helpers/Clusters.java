package pt.uminho.haslab.smcoprocessors.helpers;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;
import java.util.List;

public class Clusters extends ShareCluster {

    public Clusters(List<String> resources) throws Exception {
        super(resources);
    }

    @Override
    public TestClusterTables createTables(String tableName, String columnFamily)
            throws IOException {

        TableName tbname = TableName.valueOf(tableName);
        HTableDescriptor table = new HTableDescriptor(tbname);
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        for (HBaseAdmin admin : admins) {
            admin.createTable(table);
        }

        return new TestClusterTables(configs, tbname);
    }

}
