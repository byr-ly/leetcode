package com.eb.bi.rs.frame.recframe.resultcal.offline.sorter;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SortMapper extends Mapper<Object, Text, CompositeKey, Text> {
	
	private CompositeKey compositeKey = new CompositeKey();
	private Text outValue = new Text();
	
	
	private ArrayList<Integer> keyFieldIdxArrList = new ArrayList<Integer>();
	private String fieldDelimiter;
	
	@Override
	protected void map(Object key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String valueStr = value.toString();
		String[] fields = valueStr.split(fieldDelimiter);
		
		String keyField = fields[keyFieldIdxArrList.get(0)];		
		for(int i = 1; i < keyFieldIdxArrList.size(); ++i){			
			keyField += "|" + fields[keyFieldIdxArrList.get(i)] ;	
		}
		compositeKey.set(keyField, valueStr);
		outValue.set(valueStr);
		context.write(compositeKey, outValue);		
	}	
	
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		fieldDelimiter = conf.get("field.delimiter","\\|");
		String[] keyFieldIdxArr = conf.get("key.field.indexes","0").split(",");	
		
		for (int i = 0; i < keyFieldIdxArr.length; i++) {
			keyFieldIdxArrList.add(Integer.parseInt(keyFieldIdxArr[i]));			
		}		
	}
	
}
