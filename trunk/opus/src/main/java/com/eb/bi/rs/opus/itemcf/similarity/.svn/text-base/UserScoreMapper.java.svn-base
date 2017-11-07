package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserScoreMapper extends Mapper<Object, Text, Text, ScoreWritable> {

	/*
	 * 格式 ：用户|动漫|评分
	 */
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 3) {
			return;
		}

		String user = fields[0];
		String opusId = fields[1];
		double score = Double.parseDouble(fields[2]);

		context.write(new Text(user), new ScoreWritable(opusId, score));

	}

}
