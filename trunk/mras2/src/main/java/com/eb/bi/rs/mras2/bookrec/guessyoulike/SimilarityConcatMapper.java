package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.TextPair;

/**
 * 相似度融合
 */
public class SimilarityConcatMapper extends Mapper<Object, Text, Text, TextPair> {

	@Override
	protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length < 4) {
			return;
		}

		String keyOut = fields[0] + "|" + fields[1];
		ctx.write(new Text(keyOut), new TextPair(fields[3], fields[2]));
	}
}
