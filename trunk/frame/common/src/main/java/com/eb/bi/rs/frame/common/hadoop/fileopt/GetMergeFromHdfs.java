package com.eb.bi.rs.frame.common.hadoop.fileopt;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetMergeFromHdfs {

	/*
	 *  功能：将HDFS文件合并后下载到本地某个文件中，若本地文件不存在将创建，若存在将覆盖。
	 *  输入：hdfsPath为Hdfs位置，如：hdfs://10.1.69.179:9000
	 *      hdfsFiles要合并的HDFS文件名，支持通配符，如/user/hadoop/recsys/file?/* 
	 *      localFile为合并后的本地文件名，如：/home/user/murgefile
	 */
	public static void get(String hdfsPath, String hdfsFiles, String localFile) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsPath);
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		try
		{
			FileStatus[] inputFiles = hdfs.globStatus(new Path(hdfsFiles));
			System.out.println(inputFiles.length);
			FSDataOutputStream out = local.create(new Path(localFile));
			
			for (int i=0; i<inputFiles.length; i++)
			{
				System.out.println("get file:" +inputFiles[i].getPath().getName());
				FSDataInputStream in =  hdfs.open(inputFiles[i].getPath());
				byte buffer[] = new byte[256];
				int bytesRead = 0;
				while( (bytesRead = in.read(buffer)) > 0)
				{
					out.write(buffer, 0, bytesRead);
				}
				in.close();
			}
			out.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/*
	 *  功能：将HDFS文件合并后下载到本地某个文件中，若本地文件不存在将创建，若存在将覆盖。
	 *  输入输出与上一个get函数相同，无需hdfsPath的版本，当没有hdfspath参数时，默认读取账户下的hadoop配置文件
	 */
	public static void get(String hdfsFiles, String localFile) throws IOException
	{
		Configuration conf = new Configuration();
		String hdfsPath = conf.get("fs.default.name");
		GetMergeFromHdfs.get(hdfsPath, hdfsFiles, localFile);
	}
	
}
