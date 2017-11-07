package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 将同一用户所有有过行为的动漫两两组合
 */
public class UserScoreReducer extends Reducer<Text, ScoreWritable, Text, Text> {

	protected void reduce(Text key, Iterable<ScoreWritable> values,
			Context context) throws IOException, InterruptedException {
		List<ScoreWritable> scores = new ArrayList<ScoreWritable>();
		for (ScoreWritable value : values) {
			scores.add(new ScoreWritable(value.getId(), value.getScore()));
		}

		String idI = "";
		Text keyOut = null;
		Text valueOut = null;
		for (ScoreWritable i : scores) {
			idI = i.getId();
			double scoreI = i.getScore();
			for (ScoreWritable j : scores) {
				// 两两组合一次
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
