package org.redmaplesoft.bigdata.hadoopstudy.lesson7;

import org.apache.hadoop.io.Text;

/**
 * 针对文本键值是Text数据类型，定义自己的比较规则,按字典降序
 */
public class MyTextComparator extends Text.Comparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
}
