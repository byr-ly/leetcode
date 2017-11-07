package com.eb.bi.rs.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.client.Put;

import com.eb.bi.rs.result2hbase.BaseDriver;

public class HbaseWriteTool2 extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		// hbase表名
		String tableName = properties.getProperty("conf.hbase.table");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		// 导入表达式的分隔符
		conf.set("conf.import.split", properties.getProperty("conf.import.split"));
		// 导出表达式
		conf.set("conf.import.express", properties.getProperty("conf.import.express"));

		Job job = new Job(conf, "HbaseWrite");
		job.setNumReduceTasks(0);
		job.setJarByClass(HbaseWriteTool2.class);
		job.setMapperClass(HbaseWriteMapper2.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);

		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));		
		boolean ret = job.waitForCompletion(true);

		return ret ? 0 : 1;
	}
	
}
