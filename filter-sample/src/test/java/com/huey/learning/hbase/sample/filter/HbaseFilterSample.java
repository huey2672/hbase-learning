package com.huey.learning.hbase.sample.filter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class HbaseFilterSample {

    private static Connection connection;

    private static Table table;

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

    @Before
    public void getTable() throws IOException {
        table = connection.getTable(TableName.valueOf("order"));
    }

    @Test
    public void testValueFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("zhangsan"));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testSingleColumnValueFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("customer"),
                Bytes.toBytes("name"),
                CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("zhangsan")));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testFamilyFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new FamilyFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("cup")));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testQualifierFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new QualifierFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("name")));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testPageFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new PageFilter(10);
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testPageFilterWithStarRow() throws IOException {

        String startRow = null;
        do {
            System.out.println("page from:" + startRow);
            startRow = printPageData(10, startRow);
        } while (StringUtils.isNotEmpty(startRow));

    }

    private String printPageData(long pageSize, String startRow) throws IOException {

        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(startRow)) {
            // 由于这里的 startRow 是上一页的最后一条记录，已经查询过了，所以这里 inclusive 设置为 fasle
            scan.withStartRow(Bytes.toBytes(startRow), false);
        }
        Filter filter = new PageFilter(pageSize);
        scan.setFilter(filter);

        String rowKey = null;
        try (ResultScanner resultScanner = table.getScanner(scan)) {
            for (Result result : resultScanner) {
                // 记录 rowKey，最终保留的是本页的最后一条记录
                rowKey = Bytes.toString(result.getRow());
                System.out.println("rowKey = " + rowKey + ", " + result);
            }
        }

        return rowKey;

    }

    @Test
    public void testRowFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("202309150001")));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testPrefixFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes("202309"));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testFilterList() throws IOException {

        Scan scan = new Scan();
        Filter familyFilter = new FamilyFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("customer")));
        Filter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("name")));
        Filter singleColumnValueFilter = new ValueFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("zhangsan")));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(familyFilter);
        filterList.addFilter(qualifierFilter);
        filterList.addFilter(singleColumnValueFilter);
        scan.setFilter(filterList);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    @Test
    public void testFamilyPrefixFilter() throws IOException {

        Scan scan = new Scan();
        Filter filter = new FamilyPrefixFilter(Bytes.toBytes("p"));
        scan.setFilter(filter);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
            scanAndPrintResult(resultScanner);
        }

    }

    private void scanAndPrintResult(ResultScanner resultScanner) throws IOException {
        for (Result result : resultScanner) {
            String rowKey = Bytes.toString(result.getRow());
            System.out.println("rowKey = " + rowKey + ", " + result);
        }
    }

}
