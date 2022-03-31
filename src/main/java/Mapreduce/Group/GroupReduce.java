package Mapreduce.Group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class GroupReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String str = "";
        final List<Integer> li=new ArrayList<Integer>();
        for (Text value : values) {
            String s = value.toString();
            if (s != null) {
                int a = Integer.parseInt(s);
                li.add(a);
            }
        }
            Collections.sort(li);
            for (int i = 0; i < li.size(); i++) {
                int t = li.get(i);
                if (i != li.size() - 1)
                    str = str + li.get(i) + ",";
                else
                    str = str+li.get(i);
            }
        context.write(key, new Text(str));

    }
}
