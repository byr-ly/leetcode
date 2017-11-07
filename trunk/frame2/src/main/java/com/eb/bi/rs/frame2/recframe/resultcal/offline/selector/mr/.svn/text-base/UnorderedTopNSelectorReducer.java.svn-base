package com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.mr;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.MinHeapSortUtil;
import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.StringDoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class UnorderedTopNSelectorReducer extends Reducer<Text, StringDoublePair, NullWritable, Text> {
	private int selectNumber;
	@Override
	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws java.io.IOException ,InterruptedException {
		ArrayList<StringDoublePair> list = new ArrayList<StringDoublePair>();
		for(StringDoublePair pair : values){
			list.add(new StringDoublePair(pair));
		}
		ArrayList<StringDoublePair> topList = MinHeapSortUtil.getTopNArray(list, selectNumber);
		for(StringDoublePair pair : topList){
			context.write(NullWritable.get(), new Text(pair.getFirst()));
		}
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		selectNumber = conf.getInt("select.number", 1);
	}

}
