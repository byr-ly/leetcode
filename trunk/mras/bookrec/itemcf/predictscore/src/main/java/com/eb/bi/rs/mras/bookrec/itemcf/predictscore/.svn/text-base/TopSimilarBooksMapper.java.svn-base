package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

/**
 * 过滤掉相似度小于N的数据
 */
public class TopSimilarBooksMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * @param value 格式：源图书|目的图书|相似度|共现次数 或 源图书|目的图书|相似度
	 * 
	 */
	private float minSimilarity;
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length < 3) {
			return;
		}
		
		if (Float.parseFloat(fields[2]) < minSimilarity) {
			return;
		}
		
		context.write(new Text(fields[0]), new Text(fields[1] + "|" + fields[2]));
		context.write(new Text(fields[1]), new Text(fields[0] + "|" + fields[2]));
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {	
		Configuration conf = context.getConfiguration();
		minSimilarity =  Float.parseFloat(conf.get("conf.min.similarity", "0"));
	}
	
}
