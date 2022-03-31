package Mapreduce.Group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import java.io.IOException;

public class Group {
    public static void main(String[] args) throws
            IOException, InterruptedException, ClassNotFoundException {
        Configuration conf =new Configuration(true);
//        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job= Job.getInstance(conf,"Group");

        job.setJarByClass(Group.class);
        Path input=new Path("/tem/Group/input/");
        TextInputFormat.addInputPath(job,input);
        //Map Task
        job.setMapperClass(GroupMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //Reduce Task
        job.setReducerClass(GroupReduce.class);
//        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        Path ouput=new Path("/tem/Group/output/");
        if(ouput.getFileSystem(conf).exists(ouput))
            ouput.getFileSystem(conf).delete(ouput,true);
        TextOutputFormat.setOutputPath(job,ouput);
        job.waitForCompletion(true);
    }
}
