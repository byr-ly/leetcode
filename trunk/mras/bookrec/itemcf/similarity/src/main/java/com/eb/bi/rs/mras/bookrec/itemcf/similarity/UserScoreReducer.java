package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 将同一用户所有评论过的图书两两组合
 */
public class UserScoreReducer extends Reducer<Text, ScoreWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<ScoreWritable> values, Context context) throws IOException, InterruptedException {
		List<ScoreWritable> scores = new ArrayList<ScoreWritable>();
		for (ScoreWritable value : values) {
			scores.add(new ScoreWritable(value.getId(), value.getScore()));
		}

		String idI = null;
		Text keyOut = null;
		Text valueOut = null;
		for (ScoreWritable i : scores) {
			idI = i.getId();
			double scoreI = i.getScore();
			for (ScoreWritable j : scores) {
				// 只在idI小于idJ时才计算，即对任意两本不同的书，只组合一次
				// 使输出记录从n^2减少到n(n-1)/2
				if (idI.compareTo(j.getId()) >= 0) {
					continue;
				}
				keyOut = new Text(idI + "|" + j.getId());
				valueOut = new Text(scoreI + "|" + j.getScore());
				context.write(keyOut, valueOut);
			}
		}
	}
}

