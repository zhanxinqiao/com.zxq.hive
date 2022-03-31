package Mapreduce.FOF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyFoF {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
//        conf.set("mapreduce.framework.name","yarn");
        conf.set("mapreduce.app-submission.cross-platform","true");

        //将设设置好的conf提交
        Job job= Job.getInstance(conf,"fof");
        job.setJarByClass(MyFoF.class);
        String[] other=new GenericOptionsParser(conf,args).getRemainingArgs();
        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //mapTask
        //input
        TextInputFormat.addInputPath(job,new Path(other[0]));
        //key
        //map
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduceTask
        //reduced
        job.setReducerClass(FReducer.class);


        //output
        Path outPath=new Path(other[1]);
        if (FileSystem.get(conf).exists(outPath))
            FileSystem.get(conf).delete(outPath,true);
        TextOutputFormat.setOutputPath(job,outPath);
        //提交
        job.waitForCompletion(true);


    }

}