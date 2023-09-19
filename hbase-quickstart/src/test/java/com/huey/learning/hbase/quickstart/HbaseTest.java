package com.huey.learning.hbase.quickstart;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class HbaseTest {

    private static Connection connection;

    @BeforeClass
    public static void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
    }

    @AfterClass
    public static void release() throws IOException {
        IOUtils.close(connection);
    }

    @Test
    public void testPut() throws IOException {

        String tableName = "order";
        String rowKey = "202309150001";
        String columnFamily = "customer";
        String columnQualifier = "name";
        String value = "Huey";

        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
        table.put(put);

    }

    @Test
    public void testBatchPut() throws IOException {

        Put put1 = new Put(Bytes.toBytes("202309150002"));
        put1.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"))
                .addColumn(Bytes.toBytes("customer"), Bytes.toBytes("email"), Bytes.toBytes("zs@163.com"));

        Put put2 = new Put(Bytes.toBytes("202309150003"));
        put2.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("name"), Bytes.toBytes("lisi"))
                .addColumn(Bytes.toBytes("customer"), Bytes.toBytes("phone"), Bytes.toBytes("18912345678"));

        // 获取表对象
        Table table = connection.getTable(TableName.valueOf("order"));
        table.put(Arrays.asList(put1, put2));

    }

    @Test
    public void testGet() throws IOException {

        String tableName = "order";
        String rowKey = "202309150001";
        String columnFamily = "customer";
        String columnQualifier = "name";

        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 查询数据
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
        System.out.println("Bytes.toString(value) = " + Bytes.toString(value));

    }

    @Test
    public void testExist() throws IOException {
        Get get = new Get(Bytes.toBytes("202309150001"));
        Table table = connection.getTable(TableName.valueOf("order"));
        boolean exists = table.exists(get);
    }

    @Test
    public void testDelete() throws IOException {

        String tableName = "order";
        String rowKey = "202309150001";
        String columnFamily = "customer";
        String columnQualifier = "name";

        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
        table.delete(delete);

    }

    @Test
    public void testBatchDelete() throws IOException {

        Delete delete1 = new Delete(Bytes.toBytes("202309150001"));
        Delete delete2 = new Delete(Bytes.toBytes("202309150002"));

        Table table = connection.getTable(TableName.valueOf("order"));
        table.delete(Arrays.asList(delete1, delete2));

    }

    @Test
    public void testIncrement() throws IOException {

        Increment increment = new Increment(Bytes.toBytes("202309150004"));
        increment.addColumn(Bytes.toBytes("customer"), Bytes.toBytes("age"), 1L);

        Table table = connection.getTable(TableName.valueOf("order"));
        table.increment(increment);

    }

    @Test
    public void testScan() throws IOException {

        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes("202309150001"), true)
                .withStopRow(Bytes.toBytes("202309150005"), true);

        Table table = connection.getTable(TableName.valueOf("order"));
        try (ResultScanner resultScanner = table.getScanner(scan)) {
            for (Result result : resultScanner) {
                String rowKey = Bytes.toString(result.getRow());
                System.out.println("rowKey = " + rowKey);
            }
        }

    }

}