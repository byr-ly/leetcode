package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

/**
 * 用户源图书选取
 */
public class SourceConcatReducer extends Reducer<Text, Text, NullWritable, Text> {

	private SortedSet<String> srcSet = new TreeSet<String>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

		for (Text value : values) {
			srcSet.add(value.toString());
		}

		StringBuffer sb = new StringBuffer();
		for (String src : srcSet) {
			sb.append(src + ",");
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}

		srcSet.clear();

		ctx.write(NullWritable.get(), new Text(key.toString() + "|" + sb.toString()));
	}
}
