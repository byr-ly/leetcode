package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;
import com.hadoop.compression.lzo.LzoCodec;


public class Join4UserBookPrefFinalScoreDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();	
		
		//业务相关配置
		String value;
		if ((value = properties.getProperty("select.similarity.count"))!= null) {
			conf.setInt("select.similarity.count", Integer.parseInt(value));
		}
		if ((value = properties.getProperty("select.property.count")) != null) {
			conf.setInt("select.property.count", Integer.parseInt(value));
		}
		if ((value = properties.getProperty("weights")) != null) {
			conf.set("weights", value);
		}
		
		//mr配置相关
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}

		
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());		
		
		//输入输出相关配置		
		String recRcdInputPath = properties.getProperty("rec.rcd.input.path");
		if(recRcdInputPath == null){
			throw new RuntimeException("recommend record input path is essential");
		}
		MultipleInputs.addInputPath(job, new Path(recRcdInputPath), SequenceFileInputFormat.class, Join4UserBookPrefFinalScoreOnRecRcdMapper.class);
		
		String maxValueInputPath = properties.getProperty("max.value.input.path");
		if(maxValueInputPath == null){
			throw new RuntimeException("max value input path is essential");
		}
		MultipleInputs.addInputPath(job, new Path(maxValueInputPath), SequenceFileInputFormat.class, Join4UserBookPrefFinalScoreOnUserMaxPrefsMapper.class);
	
	
		String outputPath = properties.getProperty("output.path");
		if(outputPath == null){
			throw new RuntimeException("output path is essential");
		}
		check(outputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		job.setReducerClass(Join4UserBookPrefFinalScoreReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		
		return job.waitForCompletion(true) ? 0:1;	
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

	
	public static class KeyPartition extends Partitioner<TextPair, TextPair>{
		@Override
		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}		
	}
	
	
	
}
