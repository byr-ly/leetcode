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

public class SubDimensionDriver extends Configured  implements Tool{
	private Configuration m_conf = new Configuration();
	private String m_inputPath;
	private String m_outputPath;
	private int m_reduceNum;
	
	public SubDimensionDriver(String inputPath, String outputPath, Configuration conf, int reduceNum){
		m_inputPath = inputPath;
		m_outputPath = outputPath;
		m_conf = conf;
		m_reduceNum = reduceNum;
	}
	
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		///job设置-------------
		Job job = new Job(m_conf);
		job.setJarByClass(SubDimensionDriver.class);
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(m_inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(m_outputPath));
		//设置M-R
		job.setMapperClass(SubDimensionMapper.class);
		job.setNumReduceTasks(m_reduceNum);
		//设置输入/输出格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		///-------------------
		return 0;
	}

}
