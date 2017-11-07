package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderedTopNOrderByMultiFieldMapper extends Mapper<Object, Text, Text, Text>{
	private String fieldDelimiter;
	private ArrayList<Integer> keyFieldIdxArrList = new ArrayList<Integer>();
	
	@Override
	protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] fields = null;
		if(fieldDelimiter.equals("|")){
			fields = value.toString().split("\\|");
		}else {
			fields = value.toString().split(fieldDelimiter);
		}	
		String keyField = fields[keyFieldIdxArrList.get(0)];		
		for(int i = 1; i < keyFieldIdxArrList.size(); ++i){			
			keyField += fieldDelimiter + fields[keyFieldIdxArrList.get(i)] ;	
		}
		context.write(new Text(keyField), value);	
	}
	
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {		
		
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter","|");
		String[] keyFieldIdxArr = conf.get("key.field.indexes","0").split(",");
		for (int i = 0; i < keyFieldIdxArr.length; i++) {
			keyFieldIdxArrList.add(Integer.parseInt(keyFieldIdxArr[i]));			
		}		
	}

}
