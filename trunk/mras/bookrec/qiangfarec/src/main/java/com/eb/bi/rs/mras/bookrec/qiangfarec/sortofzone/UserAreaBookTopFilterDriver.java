package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

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

public class UserAreaBookTopFilterDriver extends BaseDriver{
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("UserAreaBookTopFilterDriver");
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
		if ((value = properties.getProperty("field.delimiter.2"))!= null) {
			conf.set("field.delimiter.2", value);
		}
		if ((value = properties.getProperty("top.n.filter")) != null) {
			conf.setInt("top.n.filter", Integer.parseInt(value) );
		}
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}		
			
		job = new Job(conf);
		job.setJarByClass(getClass());
		
		//输入相关配置		
		String userbookTotalscoreInputPath = properties.getProperty("hdfs.userbook.totalscore.input.path");
		if (userbookTotalscoreInputPath == null) {
			throw new RuntimeException("userbook total socre input path is essential");
		} 
		MultipleInputs.addInputPath(job, new Path(userbookTotalscoreInputPath), TextInputFormat.class, UserAreaBookTopFilterMapper.class);			
		
		String userBookTopNInputPath = properties.getProperty("hdfs.userbook.top.n.input.path");
		if (userBookTopNInputPath == null){
			throw new RuntimeException("userbook top n input path is essential");
		}
		MultipleInputs.addInputPath(job, new Path(userBookTopNInputPath), TextInputFormat.class, UserAreaBookTopNMapper.class);

		//输出相关配置
		String userbookAreaResultOutputPath = properties.getProperty("hdfs.userbook.area.result.output.path");
		if(userbookAreaResultOutputPath == null){
			throw new RuntimeException("user book area resule output path is essential");
		}
		check(userbookAreaResultOutputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(userbookAreaResultOutputPath));		

		job.setNumReduceTasks(Integer.parseInt(reduceNum));
		job.setReducerClass(UserAreaBookTopFilterReducer.class);

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
