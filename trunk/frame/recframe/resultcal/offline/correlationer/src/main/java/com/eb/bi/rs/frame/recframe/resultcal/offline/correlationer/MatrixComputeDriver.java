package com.eb.bi.rs.frame.recframe.resultcal.offline.correlationer;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class MatrixComputeDriver extends BaseDriver{
	public MatrixComputeDriver(){
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("MatrixComputeDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//----------------------------------------------------
		String inputPath1 = app_conf.getProperty("hdfs.input.path.1");//a-b��
		String dataformatType1 = app_conf.getProperty("Appconf.data.input.format.type1");
		
		String inputPath2 = app_conf.getProperty("hdfs.input.path.2");//b-c��
		String dataformatType2 = app_conf.getProperty("Appconf.data.input.format.type2");
		
		String workPath = app_conf.getProperty("hdfs.work.path");
		
		String outPath = app_conf.getProperty("hdfs.output.path.1");
		String whoisKey = app_conf.getProperty("Appconf.output.format.whoiskey");
		String ifhaveScore = app_conf.getProperty("Appconf.output.format.ifhavescore");
		
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		String k_v_separator = app_conf.getProperty("hadoop.io.k_v_separator");
		//--------------------------------------------------------
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", k_v_separator);
		conf.set("mapred.textoutputformat.separator",k_v_separator);
		
		conf.set("Appconf.data.input.format.type1",dataformatType1);
		conf.set("Appconf.data.input.format.type2",dataformatType2);
		
		conf.set("Appconf.output.format.whoiskey",whoisKey);
		conf.set("Appconf.output.format.ifhavescore",ifhaveScore);
		//--------------------------------------------------
		job = new Job(conf);
		job.setJarByClass(MatrixComputeDriver.class);
		
		MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, MatrixMultiple1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, MatrixMultiple2Mapper.class);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setMapOutputKeyClass(TextPair.class);
		
		check(workPath,conf);
		check(outPath,conf);
	
		FileOutputFormat.setOutputPath(job, new Path(workPath+"/matrixmultiple"));
		
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(MatrixMultipleReducer.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");		
		
		//--------------------------------------------------
		job = new Job(conf);
		job.setJarByClass(MatrixComputeDriver.class);
	
		FileInputFormat.setInputPaths(job, new Path(workPath+"/matrixmultiple/part-*"));
		//
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		//M-R-------------------
		job.setMapperClass(MatrixSumMapper.class);
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(MatrixSumReducer.class);
		
		//----------------
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//=====================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");			
		
		return 0;
	}

	public static class KeyPartition extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE)% numPartitions;
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
