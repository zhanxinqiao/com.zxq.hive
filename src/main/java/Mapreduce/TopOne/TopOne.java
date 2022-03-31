package Mapreduce.TopOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class TopOne {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration con=new Configuration(true);
//        con.set("mapreduce.app-submission.cross-platform","true");
        Job job=Job.getInstance(con,"TopOne");
        job.setJarByClass(TopOne.class);
        //Map Task
        Path input=new Path("/tem/TopOne/input/");
        TextInputFormat.addInputPath(job,input);
        job.setMapperClass(TopOneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(TopOneReduce.class);
//        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        Path output=new Path("/tem/TopOne/output");
        if(FileSystem.get(con).exists(output))
            FileSystem.get(con).delete(output,true);
        TextOutputFormat.setOutputPath(job,output);
        job.waitForCompletion(true);
    }
}
