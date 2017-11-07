package com.eb.bi.rs.frame2.common.hadoop.fileopt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class PutMergeToHdfs {
    /*
     *  功能：将本地文件合并后上传到HDFS上指定某个文件中，若HDFS上的文件不存在将创建，若存在将覆盖。
     *  输入：localFiles为本地文件名，支持通配符，如/home/user/xx/data_00?/*
     *      hdfsPath为Hdfs位置，如：hdfs://10.1.69.179:9000
     *      hdfsFile为合并后的HDFS文件名，如：/user/hadoop/recsys/file
     */
    public static void put(String localFiles, String hdfsPath, String hdfsFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsPath);
 
        //hadoop 2.0移植后增加的代码，防止hadoop.common.jar和hadoop.hdfs.jar互相覆盖生成FileSystem对象的类
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);

        try {
            FileStatus[] inputFiles = local.globStatus(new Path(localFiles));
            System.out.println(inputFiles.length);
            FSDataOutputStream out = hdfs.create(new Path(hdfsFile));

            for (int i = 0; i < inputFiles.length; i++) {
                System.out.println("put file:" + inputFiles[i].getPath().getName());
                FSDataInputStream in = local.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     *  功能：将本地文件合并后上传到HDFS上指定某个文件中，若HDFS上的文件不存在将创建，若存在将覆盖。
     *  输入输出与上一个put函数相同，无需hdfsPath的版本，当没有hdfspath参数时，默认读取账户下的hadoop配置文件
     */
    public static void put(String localFiles, String hdfsFile) throws IOException {
        Configuration conf = new Configuration();
        String hdfsPath = conf.get("fs.default.name");
        PutMergeToHdfs.put(localFiles, hdfsPath, hdfsFile);
    }
}
