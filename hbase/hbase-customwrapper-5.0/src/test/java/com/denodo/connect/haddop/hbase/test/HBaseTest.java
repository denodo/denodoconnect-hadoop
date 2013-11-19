package com.denodo.connect.haddop.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.denodo.connect.hadoop.hbase.commons.naming.ParameterNaming;

public class HBaseTest {

    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set(ParameterNaming.CONF_ZOOKEEPER_QUORUM, "192.168.0.70");
        final String port = "2181";
        if (port != null) {
            conf.set(ParameterNaming.CONF_ZOOKEEPER_CLIENTPORT, port);
        }
        conf.set("hbase.client.keyvalue.maxsize", "10485760000");
    }

    /**
     * Create a table
     */
    public static void creatTable(final String tableName, final String[] familys)
            throws Exception {
        final HBaseAdmin admin = new HBaseAdmin(conf);
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

    /**
     * Delete a table
     */
    public static void deleteTable(final String tableName) throws Exception {
        try {
            final HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (final MasterNotRunningException e) {
            e.printStackTrace();
        } catch (final ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(final String tableName, final String rowKey,
            final String family, final String qualifier, final String value) throws Exception {
        try {
            final HTable table = new HTable(conf, tableName);
            final Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(final String tableName, final String rowKey)
            throws IOException {
        final HTable table = new HTable(conf, tableName);
        final List<Delete> list = new ArrayList<Delete>();
        final Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord(final String tableName, final String rowKey) throws IOException {
        final HTable table = new HTable(conf, tableName);
        final Get get = new Get(rowKey.getBytes());
        final Result rs = table.get(get);
        for (final KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecord(final String tableName) {
        try {
            final HTable table = new HTable(conf, tableName);
            final Scan s = new Scan();
            final ResultScanner ss = table.getScanner(s);
            for (final Result r : ss) {
                for (final KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(final String[] agrs) {
        try {
            final String tablename = "documents";
            final String[] familys = { "doc", "information" };
            HBaseTest.creatTable(tablename, familys);

            // add record zkb
            HBaseTest.addRecord(tablename, "1000000005|9021596422702052|XA|A", "doc", "a", "5");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702052|XA|A", "information", "c", "90");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702052|XA|A", "information", "a", "97");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702052|XA|A", "information", "b", "87");
            // add record baoniu
            HBaseTest.addRecord(tablename, "1000000005|9021596422702053|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702053|XA|A", "information", "a", "89");

            HBaseTest.addRecord(tablename, "1000000005|9021596422702054|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702054|XA|A", "information", "a", "89");

            HBaseTest.addRecord(tablename, "1000000005|9021596422702055|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702055|XA|A", "information", "a", "89");

            HBaseTest.addRecord(tablename, "1000000005|9021596422702056|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000005|9021596422702056|XA|A", "information", "a", "89");

            HBaseTest.addRecord(tablename, "1000000006|9021596422702053|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000006|9021596422702053|XA|A", "information", "a", "89");
            HBaseTest.addRecord(tablename, "1000000007|9021596422702053|XA|A", "doc", "a", "4");
            HBaseTest.addRecord(tablename, "1000000007|9021596422702053|XA|A", "information", "a", "89");

            System.out.println("===========get one record========");
            HBaseTest.getOneRecord(tablename, "1000000005|9021596422702052|XA|A");

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseTest.delRecord(tablename, "1000000005|9021596422702054|XA|A");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}