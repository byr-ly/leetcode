package com.eb.bi.rs.frame.recframe.resultcal.offline.filter.mr;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.frame.recframe.resultcal.offline.filter.util.TextPair;


public class GroupItemFilterDriver extends BaseDriver{


	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();		
		
		String value;
		if ((value = properties.getProperty("to.filter.field.delimiter"))!= null) {
			conf.set("to.filter.field.delimiter", value);
		}
		if ((value = properties.getProperty("to.filter.key.field.index")) != null) {
			conf.setInt("to.filter.key.field.index", Integer.parseInt(value));
		}
		if ((value = properties.getProperty("to.filter.item.field.index")) != null) {
			conf.setInt("to.filter.item.field.index", Integer.parseInt(value));
		}
		
		if ((value = properties.getProperty("filter.field.delimiter"))!= null) {
			conf.set("filter.field.delimiter", value);
		}
		if ((value = properties.getProperty("filter.key.field.index")) != null) {
			conf.setInt("filter.key.field.index", Integer.parseInt(value));
		}
		if ((value = properties.getProperty("filter.item.field.index")) != null) {
			conf.setInt("filter.item.field.index", Integer.parseInt(value));
		}
		
		if ((value = properties.getProperty("filter.mode")) != null) {
			conf.set("filter.mode", value);
		}		
		
		//mr配置相关
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}		
	
		if ( (value = properties.getProperty("mapreduce.outputformat.class")) != null){
			conf.setClass("mapreduce.outputformat.class", Class.forName(value), OutputFormat.class);
		}		
		if ( (value = properties.getProperty("mapred.output.compress")) != null){
			conf.setBoolean("mapred.output.compress", Boolean.parseBoolean(value));
		}		
		if( (value = properties.getProperty("mapred.output.compression.codec")) != null){
			conf.setClass("mapred.output.compression.codec", Class.forName(value), CompressionCodec.class);
		}		
		if( (value = properties.getProperty("mapred.output.compression.type")) != null){
			 conf.set("mapred.output.compression.type", value);
		}
		
		
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		//输入输出相关配置		
		String toFilterInputPaths = properties.getProperty("to.filter.input.path");
		String toFilterInputFormat = properties.getProperty("to.filter.inputformat");
		if (toFilterInputPaths == null) {
			throw new RuntimeException("to filter input path is essential");
		} 
		if (toFilterInputFormat == null) {
			toFilterInputFormat = "TextInputFormat";
		} 
		String[] toFilterInputPathArr = toFilterInputPaths.split(",");
		for(int i = 0; i < toFilterInputPathArr.length; ++i) {
			MultipleInputs.addInputPath(job, new Path(toFilterInputPathArr[i]),  Class.forName(toFilterInputFormat).asSubclass(InputFormat.class), GroupItem2FilterMapper.class);			
		}
		
		String filterInputPaths = properties.getProperty("filter.input.path");
		String filterInputFormat = properties.getProperty("filter.inputformat");
		if (filterInputPaths == null){
			throw new RuntimeException("filter input path is essential");
		}
		if (filterInputFormat == null) {
			throw new RuntimeException("filter inputformat is essential");
		}
		
		String[] filterInputPathArr = filterInputPaths.split(",");
		for (int i = 0; i < filterInputPathArr.length; i++) {
			MultipleInputs.addInputPath(job, new Path(filterInputPathArr[i]),  Class.forName(filterInputFormat).asSubclass(InputFormat.class), GroupItemFilterMapper.class);
		}

		
		
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
		
		job.setReducerClass(GroupItemFilterReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
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
