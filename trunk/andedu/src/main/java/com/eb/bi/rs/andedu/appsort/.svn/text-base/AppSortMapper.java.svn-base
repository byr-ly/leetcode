package com.eb.bi.rs.andedu.appsort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输入数据格式： 
 * 			资源id|score
 * 输出数据格式：
 * 			 key:key value:资源id|score
 * 
 */
public class AppSortMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("\\|",-1);
		if(StringUtils.isNotBlank(value.toString()) && split.length == 2 ){
			context.write(new Text("all"), new Text(value.toString()));
		}
	}
}
