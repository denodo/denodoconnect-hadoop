package com.denodo.connect.haddop.hbase.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.denodo.connect.hadoop.hbase.commons.naming.ParameterNaming;
import com.denodo.vdb.engine.customwrapper.CustomWrapperException;

public class testInsert {

    /**
     *
     *
     * @param args
     */
    public static void main(final String[] args) throws CustomWrapperException {
        // TODO Auto-generated method stub
        // Connects to HBase server
        final String tableName = "analytics";

        final Configuration config = HBaseConfiguration.create();
        config.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, "192.168.0.70");
        final String port = "2181";
        if (port != null) {
            config.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }
        config.set("hbase.client.keyvalue.maxsize", "10485760000");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (final MasterNotRunningException e) {

            throw new CustomWrapperException("Error Master Hbase not Running: " + e.getMessage(), e);
        } catch (final ZooKeeperConnectionException e) {

            throw new CustomWrapperException("Error ZooKeeper Connection: " + e.getMessage(), e);
        } catch (final Exception e) {
            throw new CustomWrapperException("HBase is not available: " + e.getMessage(), e);
        }
        HTable table;

        // Get table metadata
        try {
            table = new HTable(config, tableName);

            final Put p = new Put(Bytes.toBytes("domain.1004"));

            // To set the value you'd like to update in the row 'myLittleRow',
            // specify the column family, column qualifier, and value of the table
            // cell you'd like to update. The column family must already exist
            // in your table schema. The qualifier can be anything.
            // All must be specified as byte arrays as hbase is all about byte
            // arrays. Lets pretend the table 'myLittleHBaseTable' was created
            // with a family 'myLittleFamily'.
            // final File file = new File("c://ziptest.zip");
            // final InputStream inputStream = new FileInputStream(file);
            // byte[] binaryContent = null;
            // binaryContent = IOUtils.toByteArray(inputStream);

            p.add(Bytes.toBytes("hour"), Bytes.toBytes("001-France"),
                    Bytes.toBytes(new String("35555")));
            // p.add(Bytes.toBytes("post"), Bytes.toBytes("body"),
            // binaryContent);
            // p.add(Bytes.toBytes("image"), Bytes.toBytes("bodyimage"),
            // binaryContent);

            table.put(p);
            System.out.println("file added");
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Create a table
     */
    public static void creatTable(final String tableName, final String[] familys)
            throws Exception {
        final Configuration config = HBaseConfiguration.create();
        config.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, "192.168.0.70");
        final String port = "2181";
        if (port != null) {
            config.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }
        config.set("hbase.client.keyvalue.maxsize", "10485760000");
        final HBaseAdmin admin = new HBaseAdmin(config);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            final HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

}
