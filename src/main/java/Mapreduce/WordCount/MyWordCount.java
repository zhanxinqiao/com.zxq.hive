package Mapreduce.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {



    //bin/hadoop command [genericOptions] [commandOptions]
    //    hadoop jar  ooxx.jar  ooxx   -D  ooxx=ooxx  inpath  outpath
    //  args :   2类参数  genericOptions   commandOptions
    //  人你有复杂度：  自己分析 args数组
    //
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

        //本地windows
        conf.set("mapreduce.framework.name","local");
        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform","true");

//        conf.set("mapreduce.framework.name","local");
//        System.out.println(conf.get("mapreduce.framework.name"));

        Job job = Job.getInstance(conf);


//        FileInputFormat.setMinInputSplitSize(job,2222);
//        job.setInputFormatClass(ooxx.class);



        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //必须必须写的
        job.setJarByClass(MyWordCount.class);

        job.setJobName("mashibing");

        //本地windows
        Path input=new Path("file:///F:\\input\\input2\\data.txt");
        TextInputFormat.addInputPath(job, input);

        //本地windows
        Path output=new Path("file:///F:\\input\\output2");
//        if (output.getFileSystem(conf).exists(output))
//            output.getFileSystem(conf).delete(output, true);
        TextOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

//        job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

}
