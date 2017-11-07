package com.eb.bi.rs.andedu.rescorrelationsplit;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * 输入数据格式： 
 * 			源资源id	关联资源id1,score|。。。|
 * 输出数据格式：
 * 			 key:源资源id|关联id1|score
 * 			 value:null
 * 
 */
public class ResCorrelationFilterResultSplitMapper extends Mapper<Object, Text, Text, NullWritable> {

	String fieldsDelimiter;
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(fieldsDelimiter);
		if(StringUtils.isNotBlank(value.toString()) && split.length == 2 ){
			String[] split2 = split[1].split("\\|");
				for(String s : split2){
					String[] split3 = s.split(",",-1);
					context.write(new Text( split[0] + "|" + split3[0] + "|" + split3[1] ), NullWritable.get());
				}
		}
	}
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration configuration = context.getConfiguration();
		fieldsDelimiter = configuration.get("fields.delimiter", ";");
	}
}
