package org.redmaplesoft.bigdata.hadoopstudy.lesson12.selfjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SelfJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        // 数据: 7566,JONES,MANAGER,7839,1981/4/2,2975,0,20
        String data = value1.toString();

        //分词操作
        String[] words = data.split(",");

        //输出数据
        //1、作为老板表                                         员工号
        context.write(new IntWritable(Integer.parseInt(words[0])), new Text("*" + words[1]));

        //2、作为员工表                                         老板的员工号
        context.write(new IntWritable(Integer.parseInt(words[3])), new Text(words[1]));
        /*
         * 注意一个问题：如果数据存在非法数据，一定处理一下（数据清洗）
         * 如果产生例外，一定捕获
         */
    }
}












