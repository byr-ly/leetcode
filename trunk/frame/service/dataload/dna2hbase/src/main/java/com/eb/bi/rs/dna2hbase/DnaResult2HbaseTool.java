package com.eb.bi.rs.dna2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

public class DnaResult2HbaseTool extends BaseDriver {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		String tableName = properties.getProperty("conf.hbase.table");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		String split = properties.getProperty("conf.text.split");
		conf.set("conf.text.split", split);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "dnaresult2hbase");

		job.setJarByClass(DnaResult2HbaseTool.class);
		job.setMapperClass(DnaResult2HbaseMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "0"));
		job.setNumReduceTasks(reduceNum);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));

		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
}
