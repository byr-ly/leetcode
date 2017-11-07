package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import org.apache.hadoop.conf.Configuration;
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

public class UnifiedRecRepTool extends BaseDriver {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		String tableName = properties.getProperty("conf.hbase.table");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "unifiedrecrep2hbase");

		job.setJarByClass(UnifiedRecRepTool.class);
		job.setMapperClass(UnifiedRecRepMapper.class);
		job.setReducerClass(UnifiedRecRepReducer.class);
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
