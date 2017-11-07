package com.eb.bi.rs.hdfs2hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class HbaseWriteReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
	private String[] columns = null;
	private String split = null;
	private String express = null;
	private int rowkeyIndex = -1;
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		split = conf.get("conf.import.split");
		express = conf.get("conf.import.express");
		columns = express.split(split, -1);
		for (int i=0; i<columns.length; i++) {
			if(columns[i].equals("rowkey")) {
				rowkeyIndex = i;
			}
		}
		// 判断配置参数异常
		if (split.isEmpty() || columns.length == 0 || rowkeyIndex <0) {
			throw new InterruptedException("config file param error");
		}
	};
	
	@Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		String[] tokens = key.toString().split(split, columns.length);
		if (tokens.length != columns.length) {
			throw new InterruptedException("Data param num and config prarm not equal");
		}
		byte[] bRowKey = Bytes.toBytes(tokens[rowkeyIndex]);
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
		Put p = new Put(bRowKey);
		for (int i=0; i<tokens.length; i++) {
			if (i == rowkeyIndex) continue;
			String[] familyAndColumn = columns[i].split(":", -1);
			if (familyAndColumn.length < 2) {
				throw new InterruptedException("cf and column format error");
			}
			p.add(Bytes.toBytes(familyAndColumn[0]), Bytes.toBytes(familyAndColumn[1]), Bytes.toBytes(tokens[i]));
		}
		context.write(rowKey, p);
	}
}

