package Mapreduce.TopN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {
    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TKey key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        //1973-6-4 33   33
        Iterator<IntWritable> iter = values.iterator();
        int flag = 0;
        int day = 0;
        while (iter.hasNext()) {
            System.out.println(key.getYear()+ key.getMonth()+ key.getDay());
            //context.nextKeyValue() ----》对key 和 value 更新值
            IntWritable val = iter.next();
            if (flag == 0) {
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay()+"@"+key.getLocation());
                rval.set(key.getWd());
                context.write(rkey, rval);
                flag++;
                day = key.getDay();
            }

            if (flag != 0 && day != key.getDay()) {
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay()+"@"+key.getLocation());
                rval.set(key.getWd());
                context.write(rkey, rval);
                break;
            }
        }

    }
}
