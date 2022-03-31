package Mapreduce.HeroWcount;

import Mapreduce.TopN.TKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class HeroMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    int a=0;
    private final Text mkey=new Text();
    private final IntWritable mval=new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,
            IntWritable>.Context context) throws IOException, InterruptedException {
        String[] strs= StringUtils.split(value.toString(),',');
//        if (a==0){
//            a=a+1;
//        }else {
            mkey.set(strs[2]);
            mval.set(Integer.parseInt(strs[5]));
            context.write(mkey,mval);
//        }
    }
}
