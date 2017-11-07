package com.eb.bi.rs.mras2.bookrec.corelationrec.selfsign;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelfSignedRecMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String va = "";
		// 源图书id;id1,score1|id2,score2|id3,score3|id4,score4|...
		String[] fields = value.toString().split("\\|", -1);
		if (fields.length == 101) {
			for (int i = 1; i < fields.length; i = i + 2) {
				va = va + fields[i] + "," + fields[i + 1] + "|";
			}
			String substring = va.substring(0, va.length() - 1);
			context.write(new Text(fields[0]), new Text(substring));
		}

	}

}
