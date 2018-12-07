package org.redmaplesoft.bigdata.hadoopstudy.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词计数主程序
 */
public class WordCountMain {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
        //1. 创建一个任务
        Job job = Job.getInstance(new Configuration());
        // 任务入口
        job.setJarByClass(WordCountMain.class);

        //2. 指定任务的map和map输出的数据类型
        job.setMapperClass(WordCountMapper.class);
        // k2的数据类型
        job.setMapOutputKeyClass(Text.class);
        // v2的数据类型
        job.setMapOutputValueClass(IntWritable.class);

        //3. 指定任务的reduce和reduce输出的数据类型
        job.setReducerClass(WordCountReducer.class);
        // k4的数据类型
        job.setOutputKeyClass(Text.class);
        // v4的数据类型
        job.setMapOutputValueClass(IntWritable.class);

        //4. 指定任务的输入路径，输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //5. 执行任务
        job.waitForCompletion(true);
    }
}
