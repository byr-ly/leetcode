package com.eb.bi.rs.algorithm.occurrence;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class ConfidenceLevelDriver extends Configured  implements Tool{
	private Configuration m_conf = new Configuration();
	private String m_inputPath;
	private String m_cachePath;
	private String m_outputPath;
	private int m_reduceNum;
	
	private Logger m_log;
	
	public ConfidenceLevelDriver(Logger log,String inputPath, String outputPath, Configuration conf, int reduceNum){
		m_log = log;
		m_inputPath = inputPath + "/cooccurrence/part*";
		m_cachePath = inputPath + "/frequency/part*";
		m_outputPath = outputPath;
		m_conf = conf;
		m_reduceNum = reduceNum;
	}
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		FileSystem fs1 = FileSystem.get(m_conf);	
		FileStatus[] status1 = fs1.globStatus(new Path(m_cachePath));
		for(int i = 0;  i  < status1.length; i++){
			DistributedCache.addCacheFile(URI.create(status1[i].getPath().toString()),m_conf);	
			m_log.info("book_and_author_name file: " + status1[i].getPath().toString() + " has been add into distributed cache");
		}
		///job设置-------------
		long start = System.currentTimeMillis();
		Job job = new Job(m_conf);
		job.setJarByClass(CooccurrenceDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(m_inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(m_outputPath));
		//设置M-R
		job.setMapperClass(ConfidenceLevelMapper.class);
		job.setNumReduceTasks(m_reduceNum);
		job.setReducerClass(ConfidenceLevelReducer.class);
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		///-------------------
		if( job.waitForCompletion(true)){
			m_log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			m_log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		m_log.info("=================================================================================");
		
		return 0;
	}

}
