package org.redmaplesoft.bigdata.hadoopstudy.lesson8.serializable.salarytotal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EmpInfoMain {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        //region 本地执行
//        System.setProperty("HADOOP_USER_NAME", "root");
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
//        config.set("fs.defaultFS", "hdfs://localhost:9000");
//        config.set("yarn.resourcemanager.hostname", "localhost");
//
//        MyFileUtil.delFolder(args[1]);
        //endregion

        Path path = new Path(args[1]);
        FileSystem fs = path.getFileSystem(config);
        //TODO: 在服务端执行无效,待查
        boolean isok = fs.deleteOnExit(path);
        if (isok) {
            System.out.println("delete file " + args[1] + " success!");
        } else {
            System.out.println("delete file " + args[1] + " failure");
        }
        //1、创建任务、指定任务的入口
        Job job = Job.getInstance(config);
        job.setJarByClass(EmpInfoMain.class);

        //2、指定任务的map和map输出的数据类型
        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Emp.class);

        // 指定自己的比较规则
        job.setSortComparatorClass(MyNumberComparator.class);

        //3、指定任务的reducer和reducer输出的类型
        job.setReducerClass(SalaryTotalReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Emp.class);

        //4、指定任务输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行任务
        job.waitForCompletion(true);

    }

}

