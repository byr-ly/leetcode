package com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Item2FilterMapper extends Mapper<Object, Text, TextPair, TextPair>{

	private int itemFieldIdx;
	private String fieldDelimiter;
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split(fieldDelimiter);
		context.write(new TextPair(fields[itemFieldIdx],"1"), new TextPair(value.toString(),"1"));
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("to.filter.field.delimiter", "\\|");
		itemFieldIdx =  conf.getInt("to.filter.item.field.index", 0);
	}
}
