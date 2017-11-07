package com.eb.bi.rs.mras.bookrec.qiangfarec.mergedata;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MergeAllDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static String datatype = "all";
	private String fieldSeparator;
	private String[] fieldsNumbers;
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] fields = value.toString().split(fieldSeparator,-1);
		String rowkey = "";
		for (String fieldsNumber : fieldsNumbers) {
			int index = Integer.parseInt(fieldsNumber); 
			rowkey += fields[index];
		}
		String result = datatype + ";" + value.toString();
		context.write(new Text(rowkey) , new Text(result));
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();		
		fieldSeparator = conf.get("fieldSeparator");
		fieldsNumbers = conf.get("duplicateRemovalFields").split(",");
	}
	
}
