package org.redmaplesoft.bigdata.hbasestudy.lesson14;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 基本数据操作单元测试
 * for HBase API 2.*
 */
public class BasicDataOperTest extends TestCase {

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

    @Test
    public void testCreateTable() throws Exception {
        //表管理类
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("emp");

        if (admin.isTableAvailable(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        if (!admin.isTableAvailable(tableName)) {
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("empinfo"));
            //获得列族描述器
            ColumnFamilyDescriptor cfd1 = cdb1.build();
            //添加列族
            tdb.setColumnFamily(cfd1);

            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("externalinfo"));
            //获得列族描述器
            ColumnFamilyDescriptor cfd2 = cdb2.build();
            //添加列族
            tdb.setColumnFamily(cfd2);

            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            //admin.addColumnFamily(tableName, cfd); //给表添加列族
            admin.createTable(td);
        }

        //关闭客户端
        admin.close();
    }

    //单条插入
    @Test
    public void testInsertOneData() throws IOException {
        TableName tableName = TableName.valueOf("emp");
        // 先插入rowkey
        Put put = new Put(Bytes.toBytes("0000"));
        //下面三个分别为，列族，列名，列值
        put.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("Max Yu"));
        put.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("80000"));
        put.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("cname"), Bytes.toBytes("俞佳玮"));
        //得到 table
        Table table = conn.getTable(tableName);
        //执行插入
        table.put(put);
    }

    //多条插入
    @Test
    public void testInsertMultiData() throws IOException {
        TableName tableName = TableName.valueOf("emp");

        //第一条数据
        // 先插入rowkey
        Put put1 = new Put(Bytes.toBytes("7369"));
        //下面三个分别为，列族，列名，列值
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("SMITH"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("8000"));

        //第二条数据
        Put put2 = new Put(Bytes.toBytes("7499"));
        put2.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("ALLEN"));
        put2.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("16000"));

        //第三条数据
        Put put3 = new Put(Bytes.toBytes("7521"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("WARD"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("12500"));

        //第四条数据
        Put put4 = new Put(Bytes.toBytes("7566"));
        put4.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("JONES"));
        put4.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("29750"));

        //第五条数据
        Put put5 = new Put(Bytes.toBytes("7654"));
        put5.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("MARTIN"));
        put5.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("12500"));

        //第六条数据
        Put put6 = new Put(Bytes.toBytes("7698"));
        put6.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("BLAKE"));
        put6.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("28500"));

        //第七条数据
        Put put7 = new Put(Bytes.toBytes("7782"));
        put7.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("CLARK"));
        put7.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("24500"));

        //第八条数据
        Put put8 = new Put(Bytes.toBytes("7788"));
        put8.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("SCOTT"));
        put8.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("30000"));

        //第九条数据
        Put put9 = new Put(Bytes.toBytes("7839"));
        put9.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("KING"));
        put9.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("50000"));

        //第十条数据
        Put put10 = new Put(Bytes.toBytes("7844"));
        put10.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("TURNER"));
        put10.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("15000"));

        //第十一条数据
        Put put11 = new Put(Bytes.toBytes("7876"));
        put11.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("ADAMS"));
        put11.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("11000"));

        //第十二条数据
        Put put12 = new Put(Bytes.toBytes("7900"));
        put12.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("JAMES"));
        put12.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("9500"));

        //第十三条数据
        Put put13 = new Put(Bytes.toBytes("7902"));
        put13.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("FORD"));
        put13.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("30000"));

        //第十四条数据
        Put put14 = new Put(Bytes.toBytes("7934"));
        put14.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("MILLER"));
        put14.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("13000"));

        //第十五条数据
        Put put15 = new Put(Bytes.toBytes("7935"));
        put15.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("Jerry"));
        put15.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("18000"));
        put15.addColumn(Bytes.toBytes("externalinfo"), Bytes.toBytes("job"), Bytes.toBytes("engineer"));

        //构造List
        List<Put> list = new ArrayList<>();
        list.add(put1);

        list.add(put3);
        list.add(put4);
        list.add(put5);
        list.add(put6);
        list.add(put7);
        list.add(put8);
        list.add(put9);
        list.add(put10);
        list.add(put11);
        list.add(put12);
        list.add(put13);
        list.add(put14);
        list.add(put15);

        //得到 table
        Table table = conn.getTable(tableName);
        //执行插入
        table.put(list);
    }


    //数据的更新
    //hbase对数据只有追加，没有更新，但是查询的时候会把最新的数据返回给哦我们
    @Test
    public void testUpdateData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("emp"));
        // 必须设置rowkey
        Put put1 = new Put(Bytes.toBytes("0000"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("85000"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"), Bytes.toBytes("70kg"));

        table.put(put1);

        table.close();
    }


    @Test
    public void testUpdateMultiData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("emp"));
        // 必须设置rowkey
        Put put1 = new Put(Bytes.toBytes("7844"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sex"), Bytes.toBytes("F"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"), Bytes.toBytes("52kg"));
        put1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("height"), Bytes.toBytes("170cm"));
        Put put2 = new Put(Bytes.toBytes("7876"));
        put2.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sex"), Bytes.toBytes("M"));
        put2.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"), Bytes.toBytes("67kg"));
        put2.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("height"), Bytes.toBytes("175cm"));

        Put put3 = new Put(Bytes.toBytes("7935"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sex"), Bytes.toBytes("M"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"), Bytes.toBytes("67kg"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("25000"));
        put3.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("Jerry"));


        List<Put> lst = new ArrayList<>();
        lst.add(put1);
        lst.add(put2);
        lst.add(put3);

        table.put(lst);

        table.close();
    }

    //删除数据
    @Test
    public void testDeleteData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("emp"));
        //参数为 row key
        //删除一列
        Delete delete1 = new Delete(Bytes.toBytes("0000"));
        delete1.addColumn(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"));

        //删除多列
        Delete delete2 = new Delete(Bytes.toBytes("7844"));
        delete2.addColumns(Bytes.toBytes("empinfo"), Bytes.toBytes("sex"));
        delete2.addColumns(Bytes.toBytes("empinfo"), Bytes.toBytes("weight"));
        //删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("7935"));
        delete3.addFamily(Bytes.toBytes("empinfo"));

        //删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("7902"));
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);
        table.close();
    }

    // 查询单条数据
    @Test
    public void testQuerySingleData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("emp"));
        //获得一行, 使用rowkey
        Get get = new Get(Bytes.toBytes("0000"));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
    }

    /**
     * 全表扫描
     * @throws IOException
     */
    @Test
    public void testScanData() throws IOException {
        Table table = conn.getTable(TableName.valueOf("emp"));
        Scan scan = new Scan();
        ResultScanner rsacn = table.getScanner(scan);
        for (Result rs : rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }





}
