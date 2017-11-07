package com.eb.bi.rs.andedu.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.frame2.algorithm.itemcf.similarity.TextPair;

public class JoinSimilarityMapper extends
		Mapper<LongWritable, Text, TextPair, Text> {
	/**
	 * value 格式：物品I|物品J|相似度
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		if (fields.length == 3) {
			context.write(new TextPair(fields[0], "0"), new Text("0|"
					+ fields[1] + "|" + fields[2]));
		}
	}

}
