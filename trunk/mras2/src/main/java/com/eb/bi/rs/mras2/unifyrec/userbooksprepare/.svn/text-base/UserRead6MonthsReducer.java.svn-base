package com.eb.bi.rs.mras2.unifyrec.userbooksprepare;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UserRead6MonthsReducer extends Reducer<Text, Text, Text, Text> {
	
	@SuppressWarnings("rawtypes")
	private MultipleOutputs mos;
	
	@SuppressWarnings("unchecked")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//存放最近6个月的30本图书，按照遗网分降序排列，存放格式为：<<book_id,adjust_score>, book_score>，其中指定排序器，按照key降序排列  
		TreeMap<TwoTuple<String, Double>, String> recentBook = new TreeMap<TwoTuple<String, Double>, String>(new Comparator<TwoTuple<String, Double>>() {
			public int compare(TwoTuple<String, Double> d1, TwoTuple<String, Double> d2) {
				if(d2.getSecond().doubleValue() >= d1.getSecond().doubleValue())
					return 1;
				else
					return -1;
		}
	});		
		//存放该用户的所有历史图书，数据格式为：<book_id, <book_score, adjust_score>>
		HashMap<String, TwoTuple<String, String>> bookScore = new HashMap<String, TwoTuple<String, String>>();
		ArrayList<String> list = new ArrayList<String>();
		for(Text value : values) {
			String[] items = value.toString().split("\\|", -1);
			if(items[0].equals("0")) {
				if(items.length >= 2) {
					for(int i = 1; i < items.length-1; i += 2) {
						list.add(items[i]);
					}	
				}
			} else if(items[0].equals("1")) {
				if(items.length >= 2) {
					for(int i = 1; i < items.length-2; i += 3) {
						TwoTuple<String, String> tmp = new TwoTuple<String, String>(items[i+1], items[i+2]);
						bookScore.put(items[i], tmp);
					}
				}
			}
		}
		
		for(int i = 0; i < list.size(); i++) {
			if(bookScore.containsKey(list.get(i))) {
				TwoTuple<String, String> tmp = bookScore.get(list.get(i));
				TwoTuple<String, Double> Key = new TwoTuple<String, Double>(list.get(i), Double.parseDouble(tmp.getSecond()));
				recentBook.put(Key, tmp.getFirst());
			}
		}
		
		StringBuffer recent = new StringBuffer();
		int count = 0;
		Iterator<Entry<TwoTuple<String, Double>, String>> iter = recentBook.entrySet().iterator();  
		Entry<TwoTuple<String, Double>, String> entry;
		while (iter.hasNext()) {   
			entry = iter.next();
			TwoTuple<String, Double> Key = entry.getKey();
			String value = entry.getValue();
			if(count++ < 30) {
				recent.append(Key.getFirst() + "," + value + "|");
				bookScore.remove(Key.getFirst());
			}
		}
		
		if(recent.length() > 0) {
			recent.deleteCharAt(recent.length() - 1);			
		}
		
		StringBuffer bookMark = new StringBuffer();
		TwoTuple<String, String> markValue;
		for(String Key : bookScore.keySet()) {
			markValue = bookScore.get(Key);
			if(Double.parseDouble(markValue.getFirst()) > 3.5) {
				bookMark.append(Key + "," + markValue.getFirst() + "|");				
			}
		}
		if(bookMark.length() > 0) {
			bookMark.deleteCharAt(bookMark.length() - 1);
		}
		if(recent.length() > 0) {
			mos.write("recentBook", key, new Text(recent.toString()));
		}
		if(bookMark.length() > 0) {
			mos.write("bookMark", key, new Text(bookMark.toString()));			
		}
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setup(Context context) {
		mos = new MultipleOutputs(context);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}



//自己实现的二元组，用来存放
class TwoTuple<A,B> {
	private A first;
	private B second;
	
	public TwoTuple(A first, B second) {
		this.first = first;
		this.second = second;
	}
	
	public A getFirst() {
		return first;
	}
	
	public void setFirst(A first) {
		this.first = first;
	}
	
	public B getSecond() {
		return second;
	}
	
	public void setSecond(B second) {
		this.second = second;
	}
	
	public String toString() {
		return "<" + first.toString() + "," + second.toString() + ">";
	}
	
}

