package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;
import org.apache.log4j.Logger;

public class GetChannelBooksDriver extends BaseDriver {

	@Override
	public int run(String[] arg0) throws Exception {
		Logger log = Logger.getLogger("GetChannelBooksDriver");
		Configuration conf = getConf();		
		
		String channelRecNum = properties.getProperty("channel.rec.num");
		conf.set("channelRecNum", channelRecNum);
		
		String increaseScore = properties.getProperty("increase.score");
		conf.set("increaseScore", increaseScore);
		
		String maxScore = properties.getProperty("max.score");
		conf.set("maxScore", maxScore);

		String reducerNum = properties.getProperty("mapred.reduce.tasks");
		if (!StringUtils.isBlank(reducerNum)) {
			conf.set("mapred.reduce.tasks", reducerNum);
		}

		String bookSimilarInfo = properties.getProperty("book.similar.info");
		if (StringUtils.isBlank(bookSimilarInfo)) {
			throw new RuntimeException("book similar info is essential");
		}
		conf.set("bookSimilarInfo", bookSimilarInfo);

		String channelRecBooksInfo = properties.getProperty("channel.rec.books.info");
		if (StringUtils.isBlank(channelRecBooksInfo)) {
			throw new RuntimeException("channel rec books info info is essential");
		}
		conf.set("channelRecBooksInfo", channelRecBooksInfo);

		Job job = new Job(conf, getClass().getSimpleName());

		FileSystem fs = FileSystem.get(URI.create(bookSimilarInfo), conf);
		FileStatus[] status = FileSystem.get(conf).globStatus(new Path(bookSimilarInfo));
		for (int i = 0; i < status.length; i++) {
			/*DistributedCache修改点*/
			job.addCacheFile(new Path(status[i].getPath().toString()).toUri());

//			DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			log.info("news num file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}

		fs = FileSystem.get(URI.create(channelRecBooksInfo), conf);
		status = fs.listStatus(new Path(channelRecBooksInfo));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(status[i].getPath().toUri(), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info(status[i].getPath().toString() + " has been add into distributedCache");
		}

		String inputPath = properties.getProperty("hdfs.input.path");
		if (StringUtils.isBlank(inputPath)) {
			throw new RuntimeException("input path is essential");
		}
		
		String outputPath = properties.getProperty("hdfs.output.path");
		if (StringUtils.isBlank(outputPath)) {
			throw new RuntimeException("output path is essential");
		}

		job.setJarByClass(getClass());
		job.setMapperClass(GetChannelBooksMapper.class);
		job.setReducerClass(GetChannelBooksReducer.class); 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, inputPath);
		check(outputPath , conf);
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
