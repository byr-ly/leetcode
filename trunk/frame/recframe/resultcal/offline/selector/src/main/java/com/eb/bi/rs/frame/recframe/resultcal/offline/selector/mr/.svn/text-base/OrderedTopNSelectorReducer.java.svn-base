package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.util.StringDoublePair;


public class OrderedTopNSelectorReducer extends Reducer<Text, StringDoublePair, NullWritable, Text >{
	private int selectorNum;
	@Override
	protected void reduce(Text key,Iterable<StringDoublePair> values, Context context) throws java.io.IOException ,InterruptedException {
		TreeSet<StringDoublePair> set = new TreeSet<StringDoublePair>();
		for(StringDoublePair pair : values){
			if(set.size() < selectorNum){
				set.add(new StringDoublePair(pair));
			}else {
				if(pair.compareTo(set.first()) > 0){
					set.remove(set.first());
					set.add(new StringDoublePair(pair));					
				}
			}
		}		
	   Iterator<StringDoublePair> iter = set.descendingIterator();
	   while(iter.hasNext()){
		   context.write(NullWritable.get(), new Text(iter.next().getFirst()));
	   }		
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		selectorNum = conf.getInt("select.number", 1);	
		
	}

}
