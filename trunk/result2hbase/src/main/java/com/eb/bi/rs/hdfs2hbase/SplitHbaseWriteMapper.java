package com.eb.bi.rs.hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitHbaseWriteMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		int lastNum = Integer.parseInt(conf.get("conf.rowkey.last"));
		String split = conf.get("conf.import.split");
		String[] tokens = value.toString().split(split, 2);
		String rowkey = tokens[0];
		if (lastNum < 0 || rowkey.length() < lastNum ) {
			lastNum = rowkey.length();
		}
		context.write(new Text(rowkey.substring(rowkey.length() - lastNum)),
				new Text(value));
	}
}
