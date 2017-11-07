package com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.filter.util.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashSet;

public class GroupItemFilterReducer extends Reducer<TextPair, TextPair, NullWritable, Text>{
	
	private String mode;
	private int toFilterItemIdx;
	private int filterItemIdx;
	private String tofilterFieldDelimiter;
	private String filterFieldDelimiter;
	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws java.io.IOException ,InterruptedException {
		HashSet<String> filterSet = new HashSet<String>();//过滤集
		
		for(TextPair pair: values){
			String record = pair.getFirst().toString();
			if(pair.getSecond().toString().equals("0")){
				String[] fields = record.split(filterFieldDelimiter);
				filterSet.add(fields[filterItemIdx]);
			}else {				
				String[] fields = record.split(tofilterFieldDelimiter);				
				if(mode.equals("exclude") && !filterSet.contains(fields[toFilterItemIdx])) {		
					context.write(NullWritable.get(), new Text(record));
				}else if(mode.equals("include") && filterSet.contains(fields[toFilterItemIdx])) {	
					context.write(NullWritable.get(), new Text(record) );
				}
			}			
		}
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		tofilterFieldDelimiter =  conf.get("to.filter.field.delimiter","\\|");
		toFilterItemIdx = conf.getInt("to.filter.item.field.index", 0);
		filterFieldDelimiter =  conf.get("filter.field.delimiter","\\|");
		filterItemIdx = conf.getInt("filter.item.field.index", 0);
		mode = conf.get("filter.mode","exclude");
	}
}
