package Mapreduce.FOF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text mkey=new Text();
    IntWritable mval=new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] strs= StringUtils.split(value.toString(),',');
        for (int i = 1; i <strs.length ; i++) {
            mkey.set(getFoF(strs[0],strs[1]));
            mval.set(0);
            context.write(mkey,mval);
            for (int j = i +1; j < strs.length; j++) {
               mkey.set(getFoF(strs[i],strs[j]));
               mval.set(1);
               context.write(mkey,mval);
            }
        }
    }
    public static String getFoF(String s1,String s2){
       if (s1.compareTo(s2)>0){
           return s1+"-"+s2;
       }else {
           return s2+"-"+s1;
       }
    }
}
