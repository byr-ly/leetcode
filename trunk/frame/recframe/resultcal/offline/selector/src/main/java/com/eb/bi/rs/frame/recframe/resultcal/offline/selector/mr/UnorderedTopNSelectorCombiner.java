package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.util.MinHeapSortUtil;
import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.util.StringDoublePair;



public class UnorderedTopNSelectorCombiner extends Reducer<Text, StringDoublePair, Text, StringDoublePair> {
	private int selectNumber;
	@Override
	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws java.io.IOException ,InterruptedException {
		ArrayList<StringDoublePair> list = new ArrayList<StringDoublePair>();
		for(StringDoublePair pair : values){
			list.add(new StringDoublePair(pair));
		}
		ArrayList<StringDoublePair> topList = MinHeapSortUtil.getTopNArray(list, selectNumber);
		for(StringDoublePair pair : topList){
			context.write(key, pair);
		}		
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		selectNumber = conf.getInt("select.number", 1);		
	}

}
