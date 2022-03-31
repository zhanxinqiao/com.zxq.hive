package Mapreduce.TopN;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.util.Date;
import java.util.HashMap;

public class TMapper extends Mapper<LongWritable, Text,TKey, IntWritable> {
   TKey mkey=new TKey();
   IntWritable mval=new IntWritable();
   public HashMap<String,String> dict=new HashMap<String,String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓冲区的值
        URI[] files=context.getCacheFiles();
        Path path=new Path(files[0].getPath());
        BufferedReader reader = new BufferedReader(new FileReader(new File(path.getName())));
        String line=reader.readLine();
        while (line!=null){
            String[] split=line.split(",");
            dict.put(split[0],split[1]);
            line=reader.readLine();
        }
    }

//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TKey, IntWritable>.Context context) throws IOException, InterruptedException {
//        //value:2019-6-1 22:22:22   1   31
//        String[] strs= StringUtils.split(value.toString(),',');
//        // 2019-6-1 22:22:22  / 1   / 31
//        //设置时间格式
//        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
//        //格式化实践并将其转换为data类型
//        try {
//            Date date = sdf.parse(strs[0]);
//            Calendar cal=Calendar.getInstance();
//            cal.setTime(date);
//            //插入mkey中 将年、月、日，温度 地点分开插入
//            mkey.setYear(cal.get(Calendar.YEAR));
//            mkey.setMonth(cal.get(Calendar.MONTH)+1);
//            mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
//            int wd=Integer.parseInt(strs[2]);
//            mkey.setWd(wd);
////        mkey.setLocation(dict.get(strs[1]));
//            //将温度作为value插入
//            mval.set(wd);
//            //将(日期(年，月，日),温度，地点的名字)作为key,温度作为value
//            context.write(mkey,mval);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//                value:2019-6-1 22:22:22   1   31
        String[] strs= StringUtils.split(value.toString(),',');
        // 2019-6-1 22:22:22  / 1   / 31
        //设置时间格式
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        //格式化实践并将其转换为data类型
        try {
            Date date = sdf.parse(strs[0]);
            Calendar cal=Calendar.getInstance();
            cal.setTime(date);
            //插入mkey中 将年、月、日，温度 地点分开插入
            mkey.setYear(cal.get(Calendar.YEAR));
            mkey.setMonth(cal.get(Calendar.MONTH)+1);
            mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int wd=Integer.parseInt(strs[2]);
            mkey.setWd(wd);
            mkey.setLocation(dict.get(strs[1]));
            //将温度作为value插入
            mval.set(wd);
            //将(日期(年，月，日),温度，地点的名字)作为key,温度作为value
            context.write(mkey,mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }
//
    }
}
