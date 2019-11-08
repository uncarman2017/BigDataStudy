package org.redmaplesoft.bigdata.hadoopstudy.lesson5;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * HDFS 基本操作测试
 */
public class test1 extends TestCase {
    private Configuration conf = new Configuration();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // 指定当前的Hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");
        // 配置参数: 指向NameNode地址
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

    }

    /**
     * 创建HDFS目录
     *
     * @throws Exception
     */
    @Test
    public void testMkDir() throws Exception {

        FileSystem fs = FileSystem.get(conf);

        boolean flag = fs.mkdirs(new Path("/inputdata"));
        System.out.println(flag);
    }

    /**
     * 上传文件
     *
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {

        // 创建一个客户端
        FileSystem fs = FileSystem.get(conf);
        // 构建一个输入流，从本地读取文件
        InputStream in = new FileInputStream("D:\\temp\\hadoop-2.7.3.tar.gz");
        // 构建一个输出流指向HDFS
        OutputStream out = fs.create(new Path("/folder111/a.tar.gz"));
        // 构造一个缓冲区
        byte[] buffer = new byte[1024];
        int len = 0;
        // 读入数据
        while (in.read(buffer) > 0) {
            // 写到输出流中
            out.write(buffer, 0, len);
        }
        out.flush();
        // 关闭流
        in.close();
        out.close();

    }

    /**
     * 下载文件
     *
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception {

        // 创建一个客户端
        FileSystem fs = FileSystem.get(conf);
        // 构建一个输入流，从HDFS读取数据
        InputStream in = fs.open(new Path("/folder111/a.tar.gz"));
        // 构建一个输出流，输出到本地目录
        OutputStream out = new FileOutputStream("d:\\temp\\xyz.tar.gz");
        //使用工具类下载
        IOUtils.copyBytes(in, out, 1024);
    }

}



