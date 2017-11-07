package com.eb.bi.rs.frame2.common.hadoop.fileopt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class PutToHdfs {
    /*
     *  功能：将本地文件上传到HDFS上指定目录，若HDFS上的目录不存在将创建，若文件存在不覆盖。
     *  输入：localFiles为本地文件名，支持通配符，如/home/user/xx/data_00?/*
     *      hdfsPath为Hdfs位置，如：hdfs://10.1.69.179:9000
     *      hdfsdir为要上传的HDFS目录名，如：/user/hadoop/recsys/dir
     */
    @SuppressWarnings("deprecation")
    public static void put(String localFiles, String hdfsPath, String hdfsdir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsPath);
        
        //hadoop 2.0移植后增加的代码，防止hadoop.common.jar和hadoop.hdfs.jar互相覆盖生成FileSystem对象的类
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
        
        
        if (hdfs.isFile(new Path(hdfsdir))) {
            hdfs.delete(new Path(hdfsdir));
        }

        if (!hdfsdir.endsWith("/")) {
            hdfsdir = hdfsdir + "/";
        }

        FileStatus[] inputFiles = local.globStatus(new Path(localFiles));
        for (FileStatus localFile : inputFiles) {
            String baseName = localFile.getPath().getName();
            String hdfsFile = hdfsdir + baseName;
            hdfs.copyFromLocalFile(localFile.getPath(), new Path(hdfsFile));
            System.out.println("copy from: " + baseName + " to " + hdfsFile);
        }
        hdfs.close();
    }

    // 无需hdfsPath的版本
    public static void put(String localFiles, String hdfsdir) throws IOException {
        Configuration conf = new Configuration();
        String hdfsPath = conf.get("fs.default.name");
        PutToHdfs.put(localFiles, hdfsPath, hdfsdir);
    }
}
