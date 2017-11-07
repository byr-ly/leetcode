package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class ChannelScoreDriver extends BaseDriver {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		String baseScore = properties.getProperty("base.score");
		conf.set("baseScore", baseScore);
		
		String increaseScore = properties.getProperty("increase.score");
		conf.set("increaseScore", increaseScore);
		
		String channelTypeAndSaleParameters = properties.getProperty("channel.type.and.sale.parameters");
		conf.set("channelTypeAndSaleParameters", channelTypeAndSaleParameters);
		
		String oldBooksPath = properties.getProperty("hdfs.old.books.info.path");
		conf.set("oldBooksPath", oldBooksPath);
		
		String newBooksPath = properties.getProperty("hdfs.new.books.info.path");
		conf.set("newBooksPath", newBooksPath);

		String bookInfoPath = properties.getProperty("hdfs.book.info.path");
		conf.set("bookInfoPath", bookInfoPath);

		String reduceNum =  properties.getProperty("mapred.reduce.tasks");
		if(reduceNum != null){
			conf.set("mapred.reduce.tasks", reduceNum);
		}

		Job job = new Job(conf,getClass().getSimpleName());

		FileSystem fs = FileSystem.get(URI.create(bookInfoPath), conf);
		FileStatus[] status = fs.listStatus(new Path(bookInfoPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
		}
		
		String filterBooksPath = properties.getProperty("filter.book.path");
		if (StringUtils.isBlank(filterBooksPath)) {
			throw new RuntimeException("filter book path is essential");
		}
		
		String inputPath = properties.getProperty("hdfs.input.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		
		String outputPath = properties.getProperty("hdfs.output.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("output path is essential");
		}

		job.setJarByClass(getClass());
		MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, ChannelScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(oldBooksPath), TextInputFormat.class, OldBooksScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(newBooksPath), TextInputFormat.class, NewBooksScoreMapper.class);
		MultipleInputs.addInputPath(job, new Path(filterBooksPath), TextInputFormat.class, FilterBooksMapper.class);
		job.setReducerClass(ChannelScoreReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		check(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		return job.waitForCompletion(true) ? 0 : -1;
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
