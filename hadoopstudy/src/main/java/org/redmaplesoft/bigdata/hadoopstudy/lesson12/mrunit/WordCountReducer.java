package org.redmaplesoft.bigdata.hadoopstudy.lesson12.mrunit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//实现Reducer的功能
//                                             k3      v3         k4       v4
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text k3, Iterable<IntWritable> v3,Context context) throws IOException, InterruptedException {
		/*
		 * context是Reducer的上下文
		 * 上文：Map
		 * 下文：HDFS
		 */
		int total = 0;
		for(IntWritable v:v3){
			//求和
			total = total + v.get();
		}

		//输出  k4  v4
		context.write(k3, new IntWritable(total));
	}
}

