package com.eb.bi.rs.frame.common.hadoop.fileopt;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetFromHdfs {

	/*
	 *  功能：将HDFS上指定目录下的文件下载到本地，若本地文件存在不覆盖。
	 *  输入：hdfsFiles为HDFS文件名，支持通配符，如/user/xx/data_00?/*
	 *      hdfsPath为Hdfs位置，如：hdfs://10.1.69.179:9000
	 *      localdir为要下载到的本地目录名，如：/home/user/recsys/dir
	 */
	public static void get(String hdfsPath, String hdfsFiles, String localdir) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsPath);
		FileSystem hdfs = FileSystem.get(conf);
		
		if(!localdir.endsWith("/")) {
			localdir = localdir + "/";
		}
		
		FileStatus[] inputFiles = hdfs.globStatus(new Path(hdfsFiles));
		for (FileStatus hdfsFile : inputFiles)
		{
			String baseName = hdfsFile.getPath().getName();
			String localPath = localdir + baseName;
			hdfs.copyToLocalFile(hdfsFile.getPath(), new Path(localPath));
			System.out.println("copy from: " + baseName + " to " + localPath);
		}
		hdfs.close();
	}

	// 无需hdfsPath的版本
	public static void get(String hdfsFiles, String localdir) throws IOException
	{
		Configuration conf = new Configuration();
		String hdfsPath = conf.get("fs.default.name");
		GetFromHdfs.get(hdfsPath, hdfsFiles, localdir);
	}
}
