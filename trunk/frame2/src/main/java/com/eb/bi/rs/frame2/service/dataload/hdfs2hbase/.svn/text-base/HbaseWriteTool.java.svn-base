package com.eb.bi.rs.frame2.service.dataload.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class HbaseWriteTool extends BaseDriver {

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
		// 表达式的分隔符
		conf.set("conf.import.split", properties.getProperty("conf.import.split"));
		// 导出表达式
		conf.set("conf.import.express", properties.getProperty("conf.import.express"));
		// 分隔字段数
		String confRowkeySplitnum = properties.getProperty("conf.import.splitnum");
		if (confRowkeySplitnum != null) {
			conf.set("conf.import.splitnum", confRowkeySplitnum);
		}
		// rowkey的规则
		String confRowkeyExpress = properties.getProperty("conf.rowkey.express");
		if (confRowkeyExpress != null) {
			conf.set("conf.rowkey.express", confRowkeyExpress);
		}
		// rowkey的分隔符
		String confRowkeySplit = properties.getProperty("conf.rowkey.split");
		if (confRowkeySplit != null) {
			conf.set("conf.rowkey.split", confRowkeySplit);
		}

		if (Boolean.parseBoolean(properties.getProperty("conf.truncate.table", "false"))) {
			// 首先要获得table的建表信息
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor td = admin.getTableDescriptor(Bytes.toBytes(tableName));
			// 删除表
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			// 重新建表 ""
			admin.createTable(td);
		}
		
		Job job = Job.getInstance(conf, "HbaseWrite"); //new Job(conf, "HbaseWrite");
		job.setNumReduceTasks(0);
		job.setJarByClass(HbaseWriteTool.class);
		job.setMapperClass(HbaseWriteMapper.class);
		job.setReducerClass(HbaseWriteReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TableOutputFormat.class);
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
