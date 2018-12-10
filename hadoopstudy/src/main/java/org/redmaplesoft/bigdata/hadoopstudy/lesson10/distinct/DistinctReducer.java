package org.redmaplesoft.bigdata.hadoopstudy.lesson10.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text k3, Iterable<NullWritable> v3, Context context) throws IOException, InterruptedException {
        // 直接把k3输出即可
        context.write(k3, NullWritable.get());
    }

}
