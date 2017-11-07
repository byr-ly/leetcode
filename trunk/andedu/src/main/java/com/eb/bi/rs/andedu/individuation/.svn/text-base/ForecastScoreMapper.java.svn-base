package com.eb.bi.rs.andedu.individuation;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 输入数据格式：
 * 			用户	资源1,推荐分1|资源2,推荐分2|...100
 * 
 * 输出数据格式：
 * 			key:用户id
 * 			value:资源1,推荐分1|资源2,推荐分2|...100
 */
public class ForecastScoreMapper extends
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
