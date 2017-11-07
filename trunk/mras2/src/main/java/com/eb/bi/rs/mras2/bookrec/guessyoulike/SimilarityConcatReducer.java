package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

/**
 * 相似度融合
 */
public class SimilarityConcatReducer extends Reducer<Text, TextPair, NullWritable, Text> {

	private Map<Integer, String> vector = new HashMap<Integer, String>();
	private int dimension;

	@Override
	protected void setup(Context ctx) throws IOException, InterruptedException {
		dimension = ctx.getConfiguration().getInt("conf.similarity.dimension", 2);
	}

	@Override
	protected void reduce(Text key, Iterable<TextPair> values, Context ctx) throws IOException, InterruptedException {
		for (TextPair value : values) {
			vector.put(Integer.parseInt(value.getSecond().toString()), value.getFirst().toString());
		}

		StringBuffer sb = new StringBuffer();
		for (int i = 1; i <= dimension; i++) {
			if (vector.containsKey(i)) {
				sb.append(vector.get(i) + ",");
			} else {
				sb.append("0,");
			}
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}

		vector.clear();

		ctx.write(NullWritable.get(), new Text(key.toString() + "|" + sb.toString()));
	}
}
