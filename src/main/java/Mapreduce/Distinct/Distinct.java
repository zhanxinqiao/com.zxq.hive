package Mapreduce.Distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Distinct {



    //bin/hadoop command [genericOptions] [commandOptions]
    //    hadoop jar  ooxx.jar  ooxx   -D  ooxx=ooxx  inpath  outpath
    //  args :   2类参数  genericOptions   commandOptions
    //  人你有复杂度：  自己分析 args数组
    //
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf,"Distinct");

        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //必须必须写的
        job.setJarByClass(Distinct.class);
        //本地windows
        Path input=new Path("/tem/Distinct/input");
        TextInputFormat.addInputPath(job, input);

        Path output=new Path("/tem/Distinct/input/output");
        if (output.getFileSystem(conf).exists(output)) output.getFileSystem(conf).delete(output, true);
        TextOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);

    }

}
