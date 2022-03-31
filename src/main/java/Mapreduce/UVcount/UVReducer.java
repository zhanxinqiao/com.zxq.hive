package Mapreduce.UVcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int a=0;
    private final Text UC=new Text("UC");
    //相同的key为一组 ，这一组数据调用一次reduce
    //hello 1
    //hello 1
    //hello 1
    //hello 1
    public void reduce(Text key, Iterable<IntWritable> values,/* 111111*/
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            if (sum==0){
                sum++;
                a++;
            }
        }
    }
    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        context.write(UC, new IntWritable(a));
    }
}
