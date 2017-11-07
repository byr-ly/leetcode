package com.eb.bi.rs.mras2.bookrec.qiangfarec.sortofzone;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *  专区在架图书编辑分表
 */
public class AreaBookEditPointsDriver extends BaseDriver {
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("SortBookEditPointsDriver");
		long start = System.currentTimeMillis();
		
		Configuration conf = new Configuration(getConf());
		
		String value;
		if ((value = properties.getProperty("field.delimiter"))!= null) {
			conf.set("field.delimiter", value);
		}
		if ((value = properties.getProperty("new.book.time")) != null) {
			conf.set("new.book.time", value);
		}
		if ((value = properties.getProperty("rec.book.edit.point")) != null) {
			conf.set("rec.book.edit.point", value );
		}
		if ((value = properties.getProperty("new.book.edit.point")) != null) {
			conf.set("new.book.edit.point", value );
		}
		
		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}		
			
		Job job = new Job(conf,getClass().getSimpleName());
		job.setJarByClass(getClass());
		
		job.setReducerClass(BookInfoEditPointsReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//输入输出相关配置		
		String bookInfoInputPath = properties.getProperty("hdfs.bookinfo.input.path");
		if (bookInfoInputPath == null) {
			throw new RuntimeException("bookinfo input path is essential");
		}
		log.info(bookInfoInputPath);
		MultipleInputs.addInputPath(job, new Path(bookInfoInputPath), TextInputFormat.class, BookInfoMapper.class);			
		
		String qiangfaBookInfoInputPath = properties.getProperty("hdfs.qiangfa.bookInfo.input.path");
		if (qiangfaBookInfoInputPath == null){
			throw new RuntimeException("qiangfa bookInfo input path is essential");
		}
		log.info(qiangfaBookInfoInputPath);
		MultipleInputs.addInputPath(job, new Path(qiangfaBookInfoInputPath), TextInputFormat.class, BookInfoQiangfaMapper.class);

		String bookInfoEditPointsoOutputPath = properties.getProperty("hdfs.bookinfo.edit.points.output.path");
		if(bookInfoEditPointsoOutputPath == null){
			throw new RuntimeException("book info edit points output path is essential");
		}
		check(bookInfoEditPointsoOutputPath,conf);
		FileOutputFormat.setOutputPath(job, new Path(bookInfoEditPointsoOutputPath));		
		
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
