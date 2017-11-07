package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class PersonalTailorNewTool extends BaseDriver {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		String tableName = properties.getProperty("conf.hbase.table");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

		Job job = new Job(conf, "personalTailorNew2hbase");

		String cachePath = properties.getProperty("conf.book.info.path");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(cachePath));
		for (FileStatus st : status) {
			job.addCacheFile(URI.create(st.getPath().toString()));
		}

		job.setJarByClass(PersonalTailorNewTool.class);
		job.setMapperClass(PersonalTailorNewMapper.class);
		job.setReducerClass(PersonalTailorNewReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));

		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
}
