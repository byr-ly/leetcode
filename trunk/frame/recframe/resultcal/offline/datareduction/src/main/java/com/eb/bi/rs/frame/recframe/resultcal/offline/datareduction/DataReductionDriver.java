package com.eb.bi.rs.frame.recframe.resultcal.offline.datareduction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class DataReductionDriver extends BaseDriver{

	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("DataReductionDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf = new Configuration(getConf());
		
		String value;
		if ((value = properties.getProperty("field.delimiter"))!= null) {
			conf.set("field.delimiter", value);
		}
		if ((value = properties.getProperty("field.delimiter.1"))!= null) {
			conf.set("field.delimiter.1", value);
		}
		if ((value = properties.getProperty("output.data.num"))!= null) {
			conf.set("output.data.num", value);
		}
		if ((value = properties.getProperty("data.pos.edit")) != null) {
			conf.set("data.pos.edit", value );
		}

		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}		

		job = new Job(conf);
		job.setJarByClass(DataReductionDriver.class);
			
		//输入输出相关配置		
		String dataInputPath = properties.getProperty("hdfs.data.input.path");
		if (dataInputPath == null) {
			throw new RuntimeException("hdfs data input path is essential");
		} 
		MultipleInputs.addInputPath(job, new Path(dataInputPath), TextInputFormat.class, DataReductionMapper.class);			

		String dataOutputPath = properties.getProperty("hdfs.data.output.path");
		if(dataOutputPath == null){
			throw new RuntimeException("hdfs data output path is essential");
		}
		check(dataOutputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(dataOutputPath));		
		
			
		job.setReducerClass(DataReductionReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 0;
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
	}
	
	public void check(String path, Configuration conf) 
	{		
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}       
    }

}
