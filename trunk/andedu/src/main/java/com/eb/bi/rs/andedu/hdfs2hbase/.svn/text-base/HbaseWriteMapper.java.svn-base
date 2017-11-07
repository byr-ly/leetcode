package com.eb.bi.rs.andedu.hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseWriteMapper extends Mapper<Object, Text, Text, NullWritable> {

	@Override
	public void map(Object o, Text value, Context context) throws IOException,
			InterruptedException {
		context.write(new Text(value), NullWritable.get());
	}
}
