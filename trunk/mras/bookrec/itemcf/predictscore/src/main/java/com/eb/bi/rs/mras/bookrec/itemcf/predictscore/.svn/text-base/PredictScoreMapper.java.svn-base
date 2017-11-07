package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 首先以用户和目的图书为key进行分发
 */
public class PredictScoreMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * @param value 格式：用户|源图书|评分|目的图书|源图书和目的图书相似度
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|", -1);
		if (fields.length == 5) {
			context.write(new Text(fields[0] + "|" + fields[3]), 
					new Text(fields[1] + "|" + fields[2] + "|" + fields[4]));
		}	
	}
}
