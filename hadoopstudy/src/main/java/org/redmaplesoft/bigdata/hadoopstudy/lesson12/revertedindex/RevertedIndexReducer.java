package org.redmaplesoft.bigdata.hadoopstudy.lesson12.revertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  k3: 单词名
 *  v3: 数据文件名:次数
 *  k4: 单词名
 *  v4: (数据文件名:次数)(数据文件名:次数), 如(wordcount01.txt:1,wordcount02.txt:2)
 */
public class RevertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text k3, Iterable<Text> v3, Context context)
            throws IOException, InterruptedException {
        String str = "";

        for (Text t : v3) {
            str = "(" + t.toString() + ")" + str;
        }

        context.write(k3, new Text(str));
    }

}
