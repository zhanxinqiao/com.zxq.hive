package Mapreduce.TopOne;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class TopOneReduce extends Reducer<Text,Text,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
        int Max=0;
        int mid=0;
        for (Text value : values) {
            String val=value.toString();
            mid=Integer.parseInt(val);
            if (mid>Max)
                Max=mid;
        }
        context.write(key,new IntWritable(Max));
    }
}
