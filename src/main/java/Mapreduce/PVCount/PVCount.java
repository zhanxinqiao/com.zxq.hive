package Mapreduce.PVCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PVCount {
    //bin/hadoop command [genericOptions] [commandOptions]
    //    hadoop jar  ooxx.jar  ooxx   -D  ooxx=ooxx  inpath  outpath
    //  args :   2类参数  genericOptions   commandOptions
    //  人你有复杂度：  自己分析 args数组
    //
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf,"PVCount");
        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //必须必须写的
        job.setJarByClass(PVCount.class);
        //本地windows
        Path input=new Path("/tem/PVCount/input");
        TextInputFormat.addInputPath(job, input);
        //本地windows
        Path output=new Path("/tem/PVCount/output");
        if (output.getFileSystem(conf).exists(output)) output.getFileSystem(conf).delete(output, true);
        TextOutputFormat.setOutputPath(job, output);

        job.setMapperClass(PVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(PVReducer.class);

//        job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

}
