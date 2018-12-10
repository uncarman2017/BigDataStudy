package org.redmaplesoft.bigdata.hadoopstudy.lesson12.revertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * * k21: 单词名:数据文件名,比如 I:data01.txt,love:data01.txt
 * * v21: 记数一次
 * k3: 单词名
 * v3: 数据文件名: 次数
 */
public class RevertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text k21, Iterable<Text> v21, Context context)
            throws IOException, InterruptedException {
        // 求和：对同一个文件中的单词进行求和
        int total = 0;
        for (Text v : v21) {
            total = total + Integer.parseInt(v.toString());
        }

        //k21是：love:data01.txt
        String data = k21.toString();
        //找到：冒号的位置
        int index = data.indexOf(":");

        //单词
        String word = data.substring(0, index);
        //文件名
        String fileName = data.substring(index + 1);

        //输出：
        context.write(new Text(word), new Text(fileName + ":" + total));
    }
}

















