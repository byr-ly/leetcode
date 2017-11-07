package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class FormatConvertUserDataDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Logger log = Logger.getLogger("FormatConvertUserDataDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		String inputPath = app_conf.getProperty("hdfs.input.path");
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//--------------------------------------------------------
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		conf.set("mapred.textoutputformat.separator","|");
		//--------------------------------------------------------
		job = Job.getInstance(conf);
		job.setJarByClass(FormatConvertUserDataDriver.class);
		
		check(outputPath,conf);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
						
		//设置M-R
		job.setMapperClass(UserDataMapper.class);
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(UserDataReducer.class);
		
		//设置输入/输出格式
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//日志==================================================================================
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

	public void check(String path, Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
	
	public static class UserDataMapper extends Mapper<NullWritable, Text, Text, Text> {
		@Override
		protected void map(NullWritable key, Text value, Context ctx) 
				throws IOException, InterruptedException {
			//用户|源图书|图书打分|来源集
			String[] fields = value.toString().split("\\|");
			
			ctx.write(new Text(fields[1]), new Text(fields[0]+"|"+fields[2]+"|"+fields[3]));
		}
	}
	
	public static class UserDataReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx) 
				throws IOException, InterruptedException {
			for (Text value: values) {
				//key:源图书;val:用户|图书打分|来源集
				ctx.write(key,value);
			}
		}
	}
}
