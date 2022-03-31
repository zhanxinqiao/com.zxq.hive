package Mapreduce.TopOne;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.StringTokenizer;
public class TopOneMapper extends Mapper<LongWritable, Text,Text,Text> {
    private final Text TopOne=new Text("TopOne");
    private final Text number=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            number.set(itr.nextToken());
            context.write(TopOne,number);;
        }
    }
}
