package org.redmaplesoft.bigdata.hadoopstudy.lesson7;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String data = value1.toString();
        String[] words = data.split(" ");
        for (String w : words) {
            context.write(new Text(w), new IntWritable(1));
        }

    }

}
