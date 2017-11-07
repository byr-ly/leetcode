package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.frame2.algorithm.itemcf.similarity.TextPair;

/*
 * 去除用户历史评分物品
 */
public class FilterPredictHistoryReducer extends
		Reducer<TextPair, Text, Text, NullWritable> {

	/*
	 * key:用户ID|物品ID value:0/1|评分
	 */
	@Override
	protected void reduce(TextPair keyPair, java.lang.Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] fields = value.toString().split("\\|", -1);
			if (fields[0].equals("0")) {
				return;
			}

			if (fields[0].equals("1")) {
				context.write(new Text(keyPair.getFirst() + "|" + fields[1]),
						NullWritable.get());
			}
		}
	}
}
