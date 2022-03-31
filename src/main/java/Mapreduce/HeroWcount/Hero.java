package Mapreduce.HeroWcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Hero {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration(true);
//        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job= Job.getInstance(conf,"HeroCount");
        job.setJarByClass(Hero.class);
        Path input=new Path("/tem/HBaseHero/input");
        TextInputFormat.addInputPath(job,input);
        //map Task
        job.setMapperClass(HeroMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce Task
        job.setReducerClass(HeroReduce.class);
        Path output=new Path("/tem/HBaseHero/output");
        if (FileSystem.get(conf).exists(output))
            FileSystem.get(conf).delete(output,true);
        TextOutputFormat.setOutputPath(job,output);
//        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //提交
        job.waitForCompletion(true);
    }
}
