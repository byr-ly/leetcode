package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.StringDoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderedTopNSelectorMapper extends Mapper<Object, Text, Text, StringDoublePair> {
	private String fieldDelimiter;
	private int[] keyFieldIdxArr;
	private int orderByFieldIdx;
	
	@Override
	protected void map(Object key, Text value,Context context) throws java.io.IOException ,InterruptedException {
		
		String[] fields = value.toString().split(fieldDelimiter);

		String keyField = fields[keyFieldIdxArr[0]];		
		for(int idx = 1; idx < keyFieldIdxArr.length; ++idx){			
			keyField += fieldDelimiter + fields[keyFieldIdxArr[idx]] ;	
		}
		String numericalValue  = fields[orderByFieldIdx];
		context.write(new Text(keyField), new StringDoublePair(value.toString(), Double.parseDouble(numericalValue)));		
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		
		String[] tmp = conf.get("key.field.indexes","0").split(",");
		keyFieldIdxArr = new int[tmp.length];
		for (int i = 0; i < tmp.length; i++) {
			keyFieldIdxArr[i] = Integer.parseInt(tmp[i]);			
		}	
		orderByFieldIdx = conf.getInt("order.by.field.index", 0);		
	}

}
