package com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.mr;


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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;
import com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.util.TextPair;


	
	
	
public class Many2ManyJoinerDriver extends BaseDriver {
	
	public int run(String[] args) throws Exception {			
		
		Configuration conf = getConf();
		
		
		String value;
		if ((value = properties.getProperty("small.side.field.delimiter"))!= null) {
			conf.set("small.side.field.delimiter", value);
		}
		if ((value = properties.getProperty("small.side.key.field.index")) != null) {
			conf.setInt("small.side.key.field.index", Integer.parseInt(value));
		}	
		
		if ((value = properties.getProperty("large.side.field.delimiter"))!= null) {
			conf.set("large.side.field.delimiter", value);
		}
		if ((value = properties.getProperty("large.side.key.field.index")) != null) {
			conf.setInt("large.side.key.field.index", Integer.parseInt(value));
		}
		
		
		if ((value = properties.getProperty("join.result.field.delimiter"))!= null) {
			conf.set("join.result.field.delimiter", value);
		}
		
		if ((value = properties.getProperty("join.result.small.side.first"))!= null) {
			conf.setBoolean("join.result.small.side.first", Boolean.parseBoolean(value));
		}	

		//mr配置相关
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}
		
		if ( (value = properties.getProperty("mapreduce.inputformat.class")) != null){
			conf.setClass("mapreduce.inputformat.class", Class.forName(value), InputFormat.class);
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
		String smallSideInputPaths = properties.getProperty("small.side.input.path");
		if(smallSideInputPaths == null){
			throw new RuntimeException("small side input path is essential");
		}else {
			String[] smallSideInputPathArr = smallSideInputPaths.split(",");
			for(int i = 0; i < smallSideInputPathArr.length; ++i) {
				MultipleInputs.addInputPath(job, new Path(smallSideInputPathArr[i]), TextInputFormat.class, Many2ManySmallSideJoinerMapper.class);
			}
		}
		
		String largeSideInputPaths = properties.getProperty("large.side.input.path");
		if(largeSideInputPaths == null){
			throw new RuntimeException("large side input path is essential");
		}else {
			String[] largeSideInputPathArr = largeSideInputPaths.split(",");
			for (int i = 0; i < largeSideInputPathArr.length; i++) {
				MultipleInputs.addInputPath(job, new Path(largeSideInputPathArr[i]), TextInputFormat.class, Many2ManyLargeSideJoinerMapper.class);
			}
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
		
		job.setReducerClass(Many2ManyJoinerReducer.class);
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
