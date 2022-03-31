package Mapreduce.HeroWcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HeroReduce extends Reducer<Text,IntWritable, IntWritable,Text> {
//    private final IntWritable result = new IntWritable();
    private final TreeMap<String, Integer> mm= new TreeMap<>();
    ValueComparator bvc =  new ValueComparator(mm);
    TreeMap<String,Integer> sorted_map = new TreeMap<String,Integer>(bvc);
    //{
//    @Override
//    public int compare(String o1, String o2) {
//        if (o1!=null&&o2!=null) {
//            int c1=mm.get(o2).compareTo(mm.get(o1));
//            if (c1 == 0)
//                return o1.compareTo(o2);
//            return c1;
//        }
//        return 0;
//    }
//}) ;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable,
            IntWritable, Text>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
//        result.set(sum);
        mm.put(String.valueOf(key),sum);
//        context.write(result,key);
    }

    @Override
    protected void cleanup(Reducer<Text, IntWritable, IntWritable, Text>.Context context)
            throws IOException, InterruptedException {
        sorted_map.putAll(mm);
        for (String s : sorted_map.keySet()) {
            context.write(new IntWritable(sorted_map.get(s)),new Text(s));
        }
    }
}
