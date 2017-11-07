package com.eb.bi.rs.frame2.algorithm.correlation.edufilter;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 输入数据格式：
 * 			源id  id1,score1|id2,score2|...
 * 
 * 输出数据格式：
 * 			key:源id
 * 			value:id1,score1|id2,score2|...
 */
public class EduCorrelationFilterMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		if(fields.length==2 && StringUtils.isNotBlank(fields[0]) && StringUtils.isNotBlank(fields[1])){
			context.write(new Text(fields[0]), new Text(fields[1]));
		}
	}
}
