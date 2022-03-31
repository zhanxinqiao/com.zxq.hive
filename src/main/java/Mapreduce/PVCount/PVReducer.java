package Mapreduce.PVCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum=0;
    private final Text PV=new Text("PV");
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        for (IntWritable val : values) {
           sum+=val.get();
        }
    }
    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        context.write(PV,new IntWritable(sum));
    }
}
