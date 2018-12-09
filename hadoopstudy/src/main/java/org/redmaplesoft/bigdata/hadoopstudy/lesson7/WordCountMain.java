package org.redmaplesoft.bigdata.hadoopstudy.lesson7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.redmaplesoft.bigdata.hadoopstudy.utils.MyFileUtil;

import java.io.File;
import java.io.IOException;

/**
 * 单词计数主程序
 */
public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration config = new Configuration();

        //region 本地执行
//        System.setProperty("HADOOP_USER_NAME", "root");
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
//        config.set("fs.defaultFS", "hdfs://localhost:9000");
//        config.set("yarn.resourcemanager.hostname", "localhost");
//        MyFileUtil.delFolder(args[1]);

        //endregion
        // 在服务端执行无效,待查
        boolean delOk = FileUtil.fullyDelete(new File(args[1]));
        if(delOk){
            System.out.println("delete folder " + args[1] + " success!");
        }
        else {
            System.out.println("delete folder " + args[1] + " failure");
        }

        //1. 创建一个任务
        //TODO: 本地执行时抛出一个异常, 但程序能继续执行
        Job job = Job.getInstance(config);
        // 任务入口
        job.setJarByClass(WordCountMain.class);

        //2. 指定任务的map和map输出的数据类型
        job.setMapperClass(WordCountMapper.class);
        // k2的数据类型
        job.setMapOutputKeyClass(Text.class);
        // v2的数据类型
        job.setMapOutputValueClass(IntWritable.class);

        //  指定自己的比较规则
        job.setSortComparatorClass(MyTextComparator.class);
        //加入Combiner
        job.setCombinerClass(WordCountReducer.class);

        //3. 指定任务的reduce和reduce输出的数据类型
        job.setReducerClass(WordCountReducer.class);
        // k4的数据类型
        job.setOutputKeyClass(Text.class);
        // v4的数据类型
        job.setMapOutputValueClass(IntWritable.class);

        //4. 指定任务的输入路径，输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5. 执行任务
        job.waitForCompletion(true);
    }
}
