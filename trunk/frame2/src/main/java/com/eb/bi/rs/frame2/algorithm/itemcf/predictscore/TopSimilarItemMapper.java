package com.eb.bi.rs.frame2.algorithm.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 过滤掉相似度小于minSimilarity的数据
 */
public class TopSimilarItemMapper extends Mapper<Object, Text, Text, Text> {

	private float minSimilarity;
	
	/**
	 * value格式：源物品|目的物品|相似度|共现次数
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if(fields.length < 3){
			return;
		}
		if(Float.parseFloat(fields[2]) < minSimilarity){
			return;
		}
		
		context.write(new Text(fields[0]), new Text(fields[1] + "|" + fields[2]));
		context.write(new Text(fields[1]), new Text(fields[0] + "|" + fields[2]));
		
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		minSimilarity = Float.parseFloat(conf.get("conf.min.similarity", "0"));
	}
}
