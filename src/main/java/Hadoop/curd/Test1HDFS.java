package Hadoop.curd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

public class Test1HDFS {
    public Configuration conf = null;
    public FileSystem fs = null;
    //获取FileSystem；方式1
    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "root");
    }

    /*
 获取FileSystem；方式2
*/
    @Test
    public void getFileSystem1() throws IOException {
        //1:创建Configuration对象
        Configuration configuration = new Configuration();
        //2:设置文件系统的类型
        configuration.set("fs.defaultFS", "hdfs://mycluster");
        //3:获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //4:输出
        System.out.println(fileSystem);
    }
    /*
    获取FileSystem；方式3
   */
    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        //指定文件系统类型
        configuration.set("fs.defaultFS", "hdfs://mycluster");

        //获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);

        System.out.println(fileSystem);
    }
    /*
    获取FileSystem  方式4
    */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://mycluster"), new Configuration());

        System.out.println(fileSystem);

    }


    //在HDFS系统上创建系统文件夹
    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("/HDFS");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    //将本地文件上传到HDFS上  方式1
    @Test
    public void upload() throws IOException {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./dict.txt")));
        Path outfile = new Path("/HDFS/out.txt");
        FSDataOutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);
    }

    //将本地文件上传到HDFS上  方式2
    @Test
    public void urlHdfs() throws IOException {

        //1:注册url
        URL.setURLStreamHandlerFactory( new FsUrlStreamHandlerFactory());
        //2:获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://mycluster").openStream();

        //3:获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello2.txt"));

        //4:实现文件的拷贝  并关闭流
        IOUtils.copyBytes(inputStream, outputStream,conf,true);

    }


    /*
  小文件的合并
 */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {

        //2:获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fs.create(new Path("/HDFS/big_txt.txt"));

        //3:获取一个本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //4:获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("F:\\Coding"));

        //5:遍历每个文件，获取每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            //6:将小文件的数据复制到大文件
            IOUtils.copyBytes(inputStream, outputStream, conf, true);
        }
        //7:关闭流
        localFileSystem.close();
    }

    /*
实现文件的下载 上传:
 */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException, InterruptedException {
        //调用方法，实现文件的下载
        fs.copyToLocalFile(new Path("/a.txt"), new Path("D://a4.txt"));
        //调用方法，实现上传
        fs.copyFromLocalFile(new Path("D://set.xml"), new Path("/"));
    }

    /*
 hdfs文件的遍历
  查看目录下文件
 */
    @Test
    public void listFiles1() throws URISyntaxException, IOException {

        //2:调用方法listFiles 获取 /目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);

        //3:遍历迭代器
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();

            //获取文件的绝对路径 : hdfs://node01:8020/xxx
            System.out.println(fileStatus.getPath() + "----" +fileStatus.getPath().getName());

            //文件的block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("block数:"+blockLocations.length);
            String isDirectory = fileStatus.isDirectory()?"文件夹":"文件";
            String permission = fileStatus.getPermission().toString();//权限
            short replication = fileStatus.getReplication();//副本系数
            long len = fileStatus.getLen();//长度
            String path = fileStatus.getPath().toString();//路径
            System.out.println(isDirectory + "\t" +permission+"\t"+ replication+"\t"+len+"\t"+path);
        }
    }



}


