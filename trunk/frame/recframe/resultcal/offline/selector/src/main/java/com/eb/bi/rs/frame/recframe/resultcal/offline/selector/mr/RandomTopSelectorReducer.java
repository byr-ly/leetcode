package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RandomTopSelectorReducer extends Reducer<Text, Text, NullWritable, Text >{
	
	private String fieldDelimiter;
	private String randBaseFieldIdxs;
	private int orderByFieldIdx;
	private int selectNum;
	private String maxOrMin;
	private boolean withSequence;
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		
		Random random = new Random();
		HashMap<String, TreeSet<String>> baseItemMap = new HashMap<String, TreeSet<String>>();
		String[] randBaseFieldIdxArr = randBaseFieldIdxs.split(",");
		for(Text value: values){
			String[] fields = null;
			if(fieldDelimiter.equals("|")){
				fields = value.toString().split("\\|");
			}else {
				fields = value.toString().split(fieldDelimiter);
			}
			String randBaseField = "";			
			for(int i = 0; i < randBaseFieldIdxArr.length; ++i){			
				randBaseField += fields[Integer.parseInt(randBaseFieldIdxArr[i])] + fieldDelimiter;	
			}
			
			if(baseItemMap.containsKey(randBaseField)){/**/
				baseItemMap.get(randBaseField).add(value.toString());
			}else{
				TreeSet<String> set = new TreeSet<String>(new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						String[] fields1 ;
						if(fieldDelimiter.equals("|")){
							fields1 = o1.split("\\|",-1);
						}else {
							fields1 = o1.split(fieldDelimiter);
						}
						String[] fields2;
						if(fieldDelimiter.equals("|")){
							fields2 = o2.split("\\|",-1);
						}else {
							fields2 = o2.split(fieldDelimiter);
						}
						double sortAccordField1 = Double.parseDouble(fields1[orderByFieldIdx]);
						double sortAccordField2 = Double.parseDouble(fields2[orderByFieldIdx]);
						if(sortAccordField1 > sortAccordField2){
							return 1;
						}else if (sortAccordField1 < sortAccordField2) {
							return -1;
						}else {
							return o1.compareTo(o2);
						}
					}
				});
				set.add(value.toString());
				baseItemMap.put(randBaseField, set);
			}			
		}
		
		int sequence = 0;
		int quotient = selectNum / baseItemMap.size();
		while(quotient > 0){
			Iterator<String> baseFieldIter = baseItemMap.keySet().iterator();
			while(baseFieldIter.hasNext()){
				String baseField = baseFieldIter.next();
				TreeSet<String> set = baseItemMap.get(baseField);
				String topRcd = set.last();
				if(maxOrMin.equals("min")){
					topRcd = set.first();
				}
				if(withSequence){
					context.write(NullWritable.get(), new Text(topRcd + fieldDelimiter + (++sequence)));
				}else {
					context.write(NullWritable.get(), new Text(topRcd));
					++sequence;
				}				
				set.remove(topRcd);
				if(set.size() == 0){
					baseFieldIter.remove();/*在遍历容器的时候，如果要删除容器中的元素，要使用迭代器删除*/
				}				
			}
			if(baseItemMap.size() == 0){				
				return;
			}else {				
				quotient = (selectNum - sequence) / baseItemMap.size();
			}			
			
		}
		
		int remainder = selectNum - sequence;
		HashSet<Integer> picked = new HashSet<Integer>();
		Object[] baseFieldArray =  baseItemMap.keySet().toArray();
		
		for(int i = 0; i < remainder; i++) {			
			int idx = random.nextInt(baseFieldArray.length);
			while(picked.contains(idx)){
				idx = random.nextInt(baseFieldArray.length);
			}
			picked.add(idx);
			TreeSet<String> set = baseItemMap.get(baseFieldArray[idx]);	
			String topRcd = set.last();
			if(maxOrMin.equals("min")){
				topRcd = set.first();
			}
			if(withSequence){
				context.write(NullWritable.get(), new Text(topRcd + fieldDelimiter + (++sequence)));
			}else {
				context.write(NullWritable.get(), new Text(topRcd));
			}
		}		
	}
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter","|");
		randBaseFieldIdxs = conf.get("random.base.field.indexes", "0");/*以逗号分隔*/
		orderByFieldIdx = conf.getInt("order.by.field.index", 0);
		selectNum = conf.getInt("select.number", 1);
		maxOrMin = conf.get("max.or.min","max");
		withSequence = conf.getBoolean("with.sequence", true);
	
	}
	

}
