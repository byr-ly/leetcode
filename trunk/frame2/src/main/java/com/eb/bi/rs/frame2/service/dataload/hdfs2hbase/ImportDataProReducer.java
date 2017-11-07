package com.eb.bi.rs.frame2.service.dataload.hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImportDataProReducer extends Reducer<Text, Text, Text, NullWritable> {

	private String rowkeyDataSplit = "";

	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		rowkeyDataSplit  = conf.get("conf.rowkey.data.split");	
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		int i = 0;
		String rowkey = "";
		for(Text value : values) {
			rowkey = key.toString()+String.valueOf(i);
			context.write(new Text(rowkey + rowkeyDataSplit + value.toString()), NullWritable.get());
			i++;
		}
	}
}
