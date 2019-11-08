package org.redmaplesoft.bigdata.hbasestudy.lesson14;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

//                                                  k3      v3      keyout代表输出的一条记录：指定行键
public class WordCountReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context)
            throws IOException, InterruptedException {
        //对v3求和
        int total = 0;
        for (IntWritable v : v3) {
            total = total + v.get();
        }

        //输出：也是表中的一条记录
        //构造一个Put对象，把单词作为rowkey
        Put put = new Put(Bytes.toBytes(k3.toString()));
        //列族
        put.addColumn(Bytes.toBytes("content"),
                // 列
                Bytes.toBytes("result"),
                //值
                Bytes.toBytes(String.valueOf(total)));

        //输出, 得到结果
        context.write(new ImmutableBytesWritable(Bytes.toBytes(k3.toString())), put);
    }

}

















