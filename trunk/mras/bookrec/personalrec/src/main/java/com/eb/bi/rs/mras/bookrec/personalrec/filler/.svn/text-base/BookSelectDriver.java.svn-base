package com.eb.bi.rs.mras.bookrec.personalrec.filler;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

/**
 * 选择最近一天的图书数据
 * */
public class BookSelectDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("BookSelectDriver");
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
		job = new Job(conf);
		job.setJarByClass(BookSelectDriver.class);
		
		check(outputPath,conf);
		
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
						
		//设置M-R
		job.setMapperClass(BookSelectMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(BookSelectReducer.class);
		
		//设置输入/输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(NullWritable.class);
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

public static class BookSelectMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	protected void map(Object key, Text value, Context ctx) 
			throws IOException, InterruptedException {
		//bookid图书|bu_type事业部|class_id分类|real_fee总费用|record_day日期
		String[] fields = value.toString().split("\\|");
		
		ctx.write(new Text(fields[0]), value);
	}
}

public static class BookSelectReducer extends Reducer<Text, Text, NullWritable, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) 
			throws IOException, InterruptedException {
		String dataString = "";
		
		int date = 0;
		
		for (Text value: values) {
			//bookid图书|bu_type事业部|class_id分类|real_fee总费用|record_day日期
			String[] fields = value.toString().split("\\|");

			if(date < Integer.valueOf(fields[4])){
				date = Integer.valueOf(fields[4]);
				dataString = value.toString();
			}
		}
		
		String record_day = date + "";
		
		dataString = dataString.substring(0, dataString.length()-record_day.length()-1);
		
		ctx.write(NullWritable.get(),new Text(dataString));
	}
}
}