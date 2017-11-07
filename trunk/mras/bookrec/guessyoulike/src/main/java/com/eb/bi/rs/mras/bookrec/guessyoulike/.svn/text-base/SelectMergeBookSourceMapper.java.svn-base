package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SelectMergeBookSourceMapper extends Mapper<NullWritable, Text, Text, Text>{
	private ArrayList<String> selectSources = new ArrayList<String>();
	
	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {
		//用户|待推图书|来源|源图书集|预测偏好向量	
		String[] fields = value.toString().split("\\|");
		if(selectSources.contains(fields[2].toLowerCase())){
			context.write(new Text(fields[0] + "|" + fields[1]), new Text(fields[3] + "|" + fields[2] + "|" + fields[4]));
		}
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {		
		String sources = context.getConfiguration().get("select.sources");
		if(sources == null) {
			throw new RuntimeException("configuration item select.sources does not exist");
		}
		String[] sourceArr = sources.split(",");
		for(String source : sourceArr) {
			selectSources.add(source.toLowerCase());
		}
	}
}
