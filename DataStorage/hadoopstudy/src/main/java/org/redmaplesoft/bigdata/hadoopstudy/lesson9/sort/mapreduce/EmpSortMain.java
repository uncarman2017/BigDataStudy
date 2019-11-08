package org.redmaplesoft.bigdata.hadoopstudy.lesson9.sort.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpSortMain {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(EmpSortMain.class);

        job.setMapperClass(EmpSortMapper.class);
        //k2为员工对象
        job.setMapOutputKeyClass(Emp.class);
        // v2是空值ֵ
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Emp.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
