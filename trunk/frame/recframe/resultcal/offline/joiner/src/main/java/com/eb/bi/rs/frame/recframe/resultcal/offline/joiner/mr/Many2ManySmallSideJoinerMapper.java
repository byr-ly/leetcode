package com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.util.TextPair;



public class Many2ManySmallSideJoinerMapper extends Mapper<Object, Text, TextPair, TextPair>{
	private String fieldDelimiter;
	private int joinKeyFieldIdx;

	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
			
		String valueStr = value.toString();
		String[] fields;
		if(fieldDelimiter.equals("|")){			
			fields = valueStr.split("\\|");
		}else {
			fields = valueStr.split(fieldDelimiter);
		}
		String joinKeyField = fields[joinKeyFieldIdx];
		String outValue;		
		if(joinKeyFieldIdx == fields.length - 1){
			outValue = valueStr.replace(fieldDelimiter + joinKeyField , "");
		}else {
			outValue = valueStr.replace(joinKeyField + fieldDelimiter, "");
		}
		context.write(new TextPair(joinKeyField, "0"), new TextPair(outValue,"0"));

	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("small.side.field.delimiter", "|");
		joinKeyFieldIdx = conf.getInt("small.side.key.field.index", 0);
	}
}
