package Mapreduce.Group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class GroupMap extends Mapper<LongWritable, Text,Text,Text> {
    private final Text kval= new Text();
    private final Text vval= new Text();
    @Override
    protected void map(LongWritable key, Text value,Context context) throws
            IOException, InterruptedException {
        String[] str= StringUtils.split(value.toString(),'\t');
        kval.set(str[0]);
        vval.set(str[1]);
        context.write(kval,vval);
    }
}
