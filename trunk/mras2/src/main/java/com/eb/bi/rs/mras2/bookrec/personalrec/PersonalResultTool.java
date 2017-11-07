package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;

//import com.ebupt.hbase.dao.TableVo;
//import com.ebupt.hbase.dao.TableDao;
//import com.ebupt.hbase.dao.DBConnection;


import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class PersonalResultTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		//conf.set("mapred.textoutputformat.separator", "|");

		String zkHost = properties.getProperty("conf.zk.host");
		conf.set("hbase.zookeeper.quorum", zkHost);
		String zkPort = properties.getProperty("conf.zk.port");
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		String tableName = properties.getProperty("conf.hbase.table");
		//String tmpTableName = tableName + "_tmp";
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

		Job job = new Job(conf, "personalresult2hbase");
		job.setJarByClass(PersonalResultTool.class);
		job.setMapperClass(PersonalResultMapper.class);
		job.setReducerClass(PersonalResultReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		int reduceNum = Integer.parseInt(properties.getProperty("conf.num.reduce.tasks", "1"));
		job.setNumReduceTasks(reduceNum);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));

		//Date yesterday = new Date(System.currentTimeMillis() - 24*60*60*1000);
		//String date = new SimpleDateFormat("yyyyMMdd").format(yesterday);
		//TableVo tab = new TableVo(tableName, tableName + "_" + date);

		//TableDao dao = new TableDao(new DBConnection(zkHost, zkPort));

		//dao.doUpdate(tab);

		//TableVo tmpTab = new TableVo(tmpTableName, tableName);
		//tmpTab.addColumnFamily(PersonalResultTable.CF);
		//dao.doCreate(tmpTab);
		
		boolean ret = job.waitForCompletion(true);
		//ret = ret && dao.doUpdate(tmpTab);

		return ret ? 0 : 1;
	}
}
