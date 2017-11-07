package com.eb.bi.rs.mras2.bookrec.channelrec.mergedata;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeIncrementDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static String datatype = "increment";
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] fields = value.toString().split("\\|",-1);//msisdn| sale_parameter|record_day
		if (fields.length == 3 && !StringUtils.isBlank(fields[0])) {	
			String msisdn = fields[0];
			String sale_parameter = fields[1];
			String result = datatype + ";" + value.toString();
			context.write(new Text(msisdn + "|" + sale_parameter) , new Text(result));
		}
	}
}
