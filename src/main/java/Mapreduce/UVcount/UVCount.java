package Mapreduce.UVcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UVCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf,"UVCount");
        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //必须必须写的
        job.setJarByClass(UVCount.class);

        Path input=new Path("/tem/UVCount/input");
        TextInputFormat.addInputPath(job, input);

        Path output=new Path("/tem/UVCount/output");
        if (output.getFileSystem(conf).exists(output)) output.getFileSystem(conf).delete(output, true);
        TextOutputFormat.setOutputPath(job, output);

        job.setMapperClass(UVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(UVReducer.class);

        job.waitForCompletion(true);

    }

}
