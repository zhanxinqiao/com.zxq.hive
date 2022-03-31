package Mapreduce.TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyTopN {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration(true);
                 //本地windows
//        conf.set("mapreduce.framework.name","local");
        conf.set("mapreduce.app-submission.cross-platform","true");
        //将设设置好的conf提交
        Job job= Job.getInstance(conf,"TopN");
        //设置提交的类
        job.setJarByClass(MyTopN.class);
        //打包的绝对路径
        job.setJar("F:\\IdeaProjects\\com.zxq.hive\\target\\com.zxq.hive-1.0-SNAPSHOT.jar");
        //获取内置的参数
//        String[] other=new GenericOptionsParser(conf,args).getRemainingArgs();
        //客户端规划的时候讲join的右表cache到mapTask出现的节点上
                //hdfs系统
        job.addCacheFile(new Path("/tem/TopNJoin/dict/dict.txt").toUri());
                    //本地windows
//        job.addCacheFile(new Path("file:///F:\\input\\dict\\dict.txt").toUri());
    //input
                   //本地windows
//        Path input=new Path("file:///F:\\input\\data.txt");
                  //hdfs系统
        Path input=new Path("/tem/TopNJoin/inputpath/");
        TextInputFormat.addInputPath(job,input);
        //mapTask
        //map类相关的设置，包括map的键、值输出类
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        //partitioner 按年 月分区 ---》 分区 > 分组 按年分区！！！！
        //分区器潜台词：满足相同的Key获得相同的分区号就可以
        job.setPartitionerClass(TPartition.class);
        //sortComparator 年 月 温度 排序 且温度倒序
        job.setSortComparatorClass(TSortComparator.class);
        //combiner   这个地方还有一个排序，这个案例不需要
        //job.setCombinerClass();
        //output
        //本地windows
//        Path output=new Path("file:///F:\\input\\output");
        //hdfs系统
        Path output=new Path("/tem/TopNJoin/outputpath");
        if (FileSystem.get(conf).exists(output))
            FileSystem.get(conf).delete(output,true);
        TextOutputFormat.setOutputPath(job,output);

        //ruducerTask
        //groupingComparator
        job.setGroupingComparatorClass(TGroupingComparator.class);
        //reducer
        job.setReducerClass(TReducer.class);



        //设置提交
        job.waitForCompletion(true);

    }
}
