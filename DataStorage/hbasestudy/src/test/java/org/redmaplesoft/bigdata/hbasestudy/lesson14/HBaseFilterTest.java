package org.redmaplesoft.bigdata.hbasestudy.lesson14;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * 常见的过滤器 for HBase API 2.*
 * 1、列值过滤器：  select * from emp where sal=3000;
 * 2、列名前缀过滤器：查询员工的姓名   select ename from emp;
 * 3、多个列名前缀过滤器：查询员工的姓名、薪水   select ename,sal from emp;
 * 4、行键过滤器：通过Rowkey查询，类似通过Get查询数据
 * 5、组合几个过滤器查询数据：where 条件1 and（or） 条件2
 */
public class HBaseFilterTest extends TestCase {

    private Configuration conf = HBaseConfiguration.create();
    private Connection conn = null;

    @Override
    public void setUp() throws Exception {
        //指定的配置信息: ZooKeeper
        conf.set("hbase.zookeeper.quorum", "172.16.100.110,172.16.22.113,172.16.22.114");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //建立连接
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void tearDown() throws Exception {
        if(conn != null) conn.close();
    }

    /**
     * 列值过滤器示例
     *
     * @throws Exception
     */
    @Test
    public void testSingleColumnValueFilter() throws Exception {
        TableName tableName = TableName.valueOf("emp");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"),
                CompareOperator.EQUAL, Bytes.toBytes("12500"));
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"))));
            System.out.println("-----------------------------------------");
        }


        table.close();
    }


    /**
     * 列名前缀过滤器示例
     *
     * @throws Exception
     */
    @Test
    public void testColumnPrefixFilter() throws Exception {

        TableName tableName = TableName.valueOf("emp");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("ename"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            //获取姓名、薪水
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            // 显示为null
            String sal = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name + "\t" + sal);
            System.out.println("-----------------------------------------");
        }

        table.close();
    }

    /**
     * 多个列名前缀过滤器示例
     *
     * @throws Exception
     */
    @Test
    public void testMultipleColumnPrefixFilter() throws Exception {
        TableName tableName = TableName.valueOf("emp");
        Table table = conn.getTable(tableName);

        //定义一个二维的字节数据，代表多个列名
        byte[][] names = {Bytes.toBytes("ename"), Bytes.toBytes("sal")};

        //定义多个列名前缀过滤器，查询：姓名和薪水
        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(names);

        //把过滤器加入扫描器
        Scan scan = new Scan();
        scan.setFilter(filter);

        //查询数据：结果中只有员工的姓名
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String rowkey = Bytes.toString(r.getRow());
            System.out.println("row key :" + rowkey);
            //获取姓名、薪水
            String name = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(r.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name + "\t" + sal);
        }

        table.close();
    }

    /**
     * rowkey过滤器示例
     *
     * @throws Exception
     */
    @Test
    public void testRowFilter() throws Exception {
        TableName tableName = TableName.valueOf("emp");
        Table table = conn.getTable(tableName);

        //定义一个RowFilter 行键过滤器: 查询rowkey=7839的员工
        Scan scan = new Scan();
//        RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("^hgs_00*"));
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("7839"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            //获取姓名、薪水
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name + "\t" + sal);
            System.out.println("------------------------------------------------");
        } //for

        table.close();
    }

    /**
     * 多种过滤器组合的例子
     * @throws Exception
     */
    @Test
    public void testFilterSet() throws Exception {
        //组合两个过滤器
        //举例：查询工资大于20000的员工姓名
        /*
         * 第一个过滤器：列值过滤器
         * 第二个过滤器：列名前缀过滤器
         */
        TableName tableName = TableName.valueOf("emp");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 =  new SingleColumnValueFilter( Bytes.toBytes("empinfo"),  Bytes.toBytes("sal"),
                CompareOperator.GREATER,  Bytes.toBytes("20000")) ;
        byte[][] names = {Bytes.toBytes("ename"), Bytes.toBytes("sal")};
        //定义多个列名前缀过滤器，查询：姓名和薪水
        MultipleColumnPrefixFilter filter2 = new MultipleColumnPrefixFilter(names);
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");

            //获取姓名、薪水
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("ename")));
            String sal = Bytes.toString(rs.getValue(Bytes.toBytes("empinfo"), Bytes.toBytes("sal")));

            System.out.println(name + "\t" + sal);
            System.out.println("-----------------------------------------");
        } //for
    }


}


















