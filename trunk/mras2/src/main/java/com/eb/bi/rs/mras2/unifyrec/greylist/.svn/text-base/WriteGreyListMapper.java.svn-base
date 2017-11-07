package com.eb.bi.rs.mras2.unifyrec.greylist;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WriteGreyListMapper extends Mapper<Object, Text, Text, NullWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		context.write(new Text(value), NullWritable.get());
	}

}
