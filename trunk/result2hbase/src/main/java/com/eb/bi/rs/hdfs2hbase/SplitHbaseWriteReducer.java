package com.eb.bi.rs.hdfs2hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class SplitHbaseWriteReducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
	private String[] columns = null;
	private String split = null;
	private String express = null;
	private int rowkeyIndex = -1;
	private long sleepms = 0;
	private int sleepns = 0;
	private long count = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String sleepstr = conf.get("conf.sleep.ms");
		String tokens[] = sleepstr.split("\\.", -1);
		sleepms = Long.parseLong(tokens[0]);
		if (tokens.length > 1) {
			sleepns = Integer.parseInt(tokens[1]); 
		}
		System.out.println("sleepms: " + sleepms + " sleepns: " + sleepns);
		split = conf.get("conf.import.split");
		express = conf.get("conf.import.express");
		columns = express.split(split, -1);
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].equals("rowkey")) {
				rowkeyIndex = i;
			}
		}
		// 判断配置参数异常
		if (split.isEmpty() || columns.length == 0 || rowkeyIndex < 0) {
			throw new InterruptedException("config file param error");
		}
	};

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] tokens = value.toString().split(split, -1);
			if (tokens.length != columns.length) {
				throw new InterruptedException(
						"Data param num and config prarm not equal");
			}
			byte[] bRowKey = Bytes.toBytes(tokens[rowkeyIndex]);
			ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
			Put p = new Put(bRowKey);
			for (int i = 0; i < tokens.length; i++) {
				if (i == rowkeyIndex)
					continue;
				String[] familyAndColumn = columns[i].split(":", -1);
				if (familyAndColumn.length < 2) {
					throw new InterruptedException("cf and column format error");
				}
				p.add(Bytes.toBytes(familyAndColumn[0]),
						Bytes.toBytes(familyAndColumn[1]),
						Bytes.toBytes(tokens[i]));
			}
			context.write(rowKey, p);
			Thread.sleep(sleepms, sleepns);
			//if(++count >= 100) {
			//	Thread.sleep(sleepms);
			//	count = 0;
			//}
		}
	}
}
