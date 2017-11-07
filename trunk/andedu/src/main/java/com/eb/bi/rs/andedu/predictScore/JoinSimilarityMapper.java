package com.eb.bi.rs.andedu.predictScore;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {
	Double limit =null;
	@Override
	/*
	 * 输入格式：品牌|目的品牌|相似度
	 * 输出格式：key：用户       value：1|目的品牌|相似度
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|", -1);
		//相似度低于阈值limit的过滤掉
		if( fields.length == 3&&Double.parseDouble(fields[2])>limit){
			context.write(new Text(fields[0]), new Text("1|" + fields[1] + "|" + fields[2]));
		}	
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		limit= conf.getDouble("similarity_limit",0.0);
	}
}
