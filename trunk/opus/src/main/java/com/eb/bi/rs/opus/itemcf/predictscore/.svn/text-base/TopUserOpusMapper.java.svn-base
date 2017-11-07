package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算每个用户的TopN个评分的动漫
 */
public class TopUserOpusMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * value 格式：用户|动漫|评分
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String fields[] = value.toString().split("\\|");
		if (fields.length != 3) {
			return;
		}
		context.write(new Text(fields[0]),
				new Text(fields[1] + "|" + fields[2]));

	}
}
