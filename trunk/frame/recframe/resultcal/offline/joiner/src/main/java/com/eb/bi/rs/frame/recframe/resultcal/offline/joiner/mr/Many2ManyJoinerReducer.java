package com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.mr;

import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.frame.recframe.resultcal.offline.joiner.util.TextPair;



public class Many2ManyJoinerReducer extends Reducer<TextPair, TextPair, NullWritable, Text >{
	
	private boolean smallSideValueFirst;
	private String joinResultFieldDelimiter;
	private String smallSideFieldDelimiter;
	private String largeSideFieldDelimiter;
	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws java.io.IOException ,InterruptedException {
		
		String joinKey = key.getFirst().toString();
		HashSet<String> set = new HashSet<String>();		
		for(TextPair pair : values){
			if(pair.getSecond().toString().equals("0")){
				if(!smallSideFieldDelimiter.equals(joinResultFieldDelimiter)){
					set.add(pair.getFirst().toString().replace(smallSideFieldDelimiter, joinResultFieldDelimiter));
				}else {
					set.add(pair.getFirst().toString());
				}
			}else {
				String value = pair.getFirst().toString();
				if(!largeSideFieldDelimiter.equals(joinResultFieldDelimiter)){
					value = pair.getFirst().toString().replace(largeSideFieldDelimiter, joinResultFieldDelimiter);
				}
				for(String item : set){
					if(smallSideValueFirst){
						//context.write(new Text(joinKey + joinResultFieldDelimiter + item + joinResultFieldDelimiter + value) , NullWritable.get());
						context.write(NullWritable.get(), new Text(joinKey + joinResultFieldDelimiter + item + joinResultFieldDelimiter + value));
					}else {
						//context.write(new Text(joinKey + joinResultFieldDelimiter + value + joinResultFieldDelimiter + item) , NullWritable.get());
						context.write(NullWritable.get(), new Text(joinKey + joinResultFieldDelimiter + value + joinResultFieldDelimiter + item));
					}
				}				
			}			
		}
	}
	
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();		
		joinResultFieldDelimiter = conf.get("join.result.field.delimiter", "|");
		smallSideFieldDelimiter = conf.get("small.side.field.delimiter", "|");
		largeSideFieldDelimiter = conf.get("large.side.field.delimiter", "|");
		smallSideValueFirst = conf.getBoolean("join.result.small.side.first", true);		
	}
}
