package org.redmaplesoft.bigdata.hadoopstudy.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 部门工资合计主程序
 */
public class SalaryTotalMain {

    public static void main(String[] args) throws Exception {
        //1、创建任务、指定任务的入口
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SalaryTotalMain.class);

        //2、指定任务的map和map输出的数据类型
        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //3、指定任务的reducer和reducer输出的类型
        job.setReducerClass(SalaryTotalReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //4、指定任务输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行任务
        job.waitForCompletion(true);
    }
}












