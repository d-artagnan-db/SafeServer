package pt.uminho.haslab.saferegions.helpers;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class Clusters extends ShareCluster {

    public Clusters(List<String> resources, int nRegionServers)
            throws Exception {
        super(resources, nRegionServers);
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

    public TestClusterTables createTables(String tableName,
                                          List<String> columnFamilies) throws IOException {

        TableName tbName = TableName.valueOf(tableName);
        HTableDescriptor table = new HTableDescriptor(tbName);

        for (String fam : columnFamilies) {
            HColumnDescriptor famDesc = new HColumnDescriptor(fam);
            table.addFamily(famDesc);
        }

        for (HBaseAdmin admin : admins) {
            admin.createTable(table);
        }

        return new TestClusterTables(configs, tbName);

    }

    public TestClusterTables createTables(String tableName,
                                          String columnFamily, List<Integer> splitKeys) throws IOException {
        TableName tbname = TableName.valueOf(tableName);
        HTableDescriptor table = new HTableDescriptor(tbname);
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        byte[][] bSplitKeys = new byte[splitKeys.size()][];

        for (int i = 0; i < splitKeys.size(); i++) {
            bSplitKeys[i] = splitKeys.get(i).toString().getBytes();
        }

        for (HBaseAdmin admin : admins) {
            admin.createTable(table, bSplitKeys);
        }
        return new TestClusterTables(configs, tbname);
    }

    public TestClusterTables createTablesByteList(String tableName,
                                                  List<String> columnFamilies, List<byte[]> splitKeys) throws IOException {

        TableName tbName = TableName.valueOf(tableName);
        HTableDescriptor table = new HTableDescriptor(tbName);
        for (String fam : columnFamilies) {
            HColumnDescriptor famDesc = new HColumnDescriptor(fam);
            table.addFamily(famDesc);
        }

        byte[][] bSplitKeys = new byte[splitKeys.size()][];

        for (int i = 0; i < splitKeys.size(); i++) {
            bSplitKeys[i] = splitKeys.get(i);
        }

        for (HBaseAdmin admin : admins) {
            admin.createTable(table, bSplitKeys);
        }
        return new TestClusterTables(configs, tbName);
    }

    public TestClusterTables newClusterTablesClient(String tableName)
            throws IOException {
        TableName tbname = TableName.valueOf(tableName);
        return new TestClusterTables(configs, tbname);

    }

    public void splitTables(String tableName, BigInteger[] splitKeys)
            throws IOException, InterruptedException {
        TableName tbname = TableName.valueOf(tableName);
        for (int i = 0; i < this.admins.size(); i++) {
            this.admins.get(i).split(tbname.getName(),
                    splitKeys[i].toByteArray());
        }
    }

}
