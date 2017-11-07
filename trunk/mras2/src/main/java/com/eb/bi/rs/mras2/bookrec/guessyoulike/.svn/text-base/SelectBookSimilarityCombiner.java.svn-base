package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.MinHeapSortUtil;
import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.StringDoublePair;

public class SelectBookSimilarityCombiner extends Reducer<Text, StringDoublePair, Text, StringDoublePair> {
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
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		selectNumber = conf.getInt("select.number", 1);		
	}

}
