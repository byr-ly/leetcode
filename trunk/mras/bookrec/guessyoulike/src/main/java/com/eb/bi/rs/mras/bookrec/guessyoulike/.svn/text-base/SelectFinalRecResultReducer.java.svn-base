package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.StringDoublePair;


public class SelectFinalRecResultReducer extends Reducer<Text, StringDoublePair, NullWritable, Text> {
	private int selectorNum;
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	
	@Override
	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws IOException ,InterruptedException {
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
		StringBuffer sb = new StringBuffer();
		sb.append(key + "|");	
		Iterator<StringDoublePair> iter = set.descendingIterator();
		  
	   
		while(iter.hasNext()) {
			StringDoublePair pair = iter.next();		
			String record = pair.getFirst();
			String[] fields = record.split("\\|");		   
			sb.append(fields[0] + "||");
			multipleOutputs.write(NullWritable.get(), new Text(key + "|" + record + "|" + pair.getSecond()), "hive/part");
		}
		sb.deleteCharAt(sb.length() - 1);
		multipleOutputs.write(NullWritable.get(), new Text(sb.toString()), "huawei/part");
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		selectorNum = conf.getInt("select.number", 1);	
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		multipleOutputs.close();
	}

}


