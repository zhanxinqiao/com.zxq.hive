package Mapreduce.TopN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartition extends Partitioner<TKey, IntWritable> {
    @Override
    public int getPartition(TKey tKey, IntWritable intWritable, int i) {
        //不能太复杂
        //按 年 月 分区  ----》 分区　＞　分组　按年分区
        //分区器潜台词  相同的key获得相同的分区号就可以
        return tKey.getYear() % i;//数据量大时需要注意数据倾斜

    }
}
