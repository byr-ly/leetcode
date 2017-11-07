package com.eb.bi.rs.algorithm.occurrence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class Vertical2horizontalDriver extends Configured  implements Tool{
	private Configuration m_conf = new Configuration();
	private String m_inputPath;
	private String m_outputPath;
	private int m_reduceNum;
	
	private Logger m_log;
	
	public Vertical2horizontalDriver(Logger log, String inputPath, String outputPath, Configuration conf, int reduceNum){
		m_log = log;
		m_inputPath = inputPath;
		m_outputPath = outputPath;
		m_conf = conf;
		m_reduceNum = reduceNum;
	}
	
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		///job设置-------------
		long start = System.currentTimeMillis();
		Job job = new Job(m_conf);
		job.setJarByClass(Vertical2horizontalDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(m_inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(m_outputPath));
		//设置M-R
		job.setMapperClass(Vertical2horizontalMapper.class);
		job.setNumReduceTasks(m_reduceNum);
		job.setReducerClass(Vertical2horizontalReducer.class);
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
