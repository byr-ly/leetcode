package com.eb.bi.rs.andedu.itemcf.similarity;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemAvgMapper extends Mapper<Object, Text, Text, FloatWritable> {

	/**
	 * value 格式：用户|物品|评分
	 */
	@Override
	protected void map(Object key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if(fields.length == 3){
			String item = fields[1];
			float score = Float.parseFloat(fields[2]);
			context.write(new Text(item), new FloatWritable(score));
		}
	}
}
