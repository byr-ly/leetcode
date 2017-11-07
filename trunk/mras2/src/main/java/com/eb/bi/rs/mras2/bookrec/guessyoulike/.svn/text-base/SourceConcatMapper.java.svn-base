package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

/**
 * 用户源图书选取 
 */
public class SourceConcatMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length < 3) {
			return;
		}

		Text keyOut = new Text(fields[0] + "|" + fields[1]);
		Text valueOut = new Text(fields[2]);
		ctx.write(keyOut, valueOut);
	}
}