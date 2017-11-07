package com.eb.bi.rs.frame.recframe.resultcal.offline.selector.mr;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderedTopNOrderByMultiFieldReducer extends Reducer<Text, Text, NullWritable, Text>{
	private String fieldDelimiter;	
	private int topNNumber;
	private int  lowBound;
	private boolean isVertical;
	private boolean withSequence;	
	private ArrayList<Integer> reserveFieldIdxArrList;
	private ArrayList<Integer> keyFieldIdxArrList;;
	private HashMap<Integer, String> orderMode = new HashMap<Integer, String>();/*2:asc,6:asc*/
	
	
	@Override
	protected void reduce(Text key,Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
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
				Iterator<Entry<Integer, String>> iter = orderMode.entrySet().iterator();
				while(iter.hasNext()){	
					Entry<Integer, String> entry = iter.next();
					double orderByField1 = Double.parseDouble(fields1[entry.getKey()]);
					double orderByField2 = Double.parseDouble(fields2[entry.getKey()]);
					if( entry.getValue().equalsIgnoreCase("asc")){
						if(orderByField1 > orderByField2) {
							return 1;
						}else if (orderByField1 < orderByField2) {
							return -1;
						}						
					}else {
						if( orderByField2 > orderByField1 ){
							return 1;
						}else if (orderByField2 <orderByField1) {
							return -1;
						}						
					}					
				}
				return o1.compareTo(o2);
			}
		});
		
//		for(Text value : values){
//			String valueStr = value.toString();
//			if(set.size() < topNNumber){
//				set.add(valueStr);
//			}else {
//				if(valueStr.compareTo(set.last()) < 0){
//					set.remove(set.last());
//					set.add(valueStr);					
//				}
//			}
//		}
		
		for(Text value : values){			
			set.add(value.toString());
			if(set.size() > topNNumber){
				set.remove(set.last());
			}
		}
		if(set.size() < lowBound) return ;
		Iterator<String> iter = set.iterator();
		if(isVertical){//竖表
			if(withSequence){
				if(reserveFieldIdxArrList != null){
					int sequence = 0;
					while(iter.hasNext()){			
						String rcd = iter.next();
						String[] fields ;
						if(fieldDelimiter.equals("|")){
							fields = rcd.split("\\|",-1);
						}else {
							fields = rcd.split(fieldDelimiter);
						}						
						String result = fields[reserveFieldIdxArrList.get(0)];
						for(int i = 1; i < reserveFieldIdxArrList.size(); ++i){
							result += fieldDelimiter + fields[reserveFieldIdxArrList.get(i)];
						}
						result += fieldDelimiter + (++sequence);				
						//context.write(new Text(result),NullWritable.get());	
						context.write(NullWritable.get(), new Text(result));		
					}				
				}else {
					int sequence = 0;
					while(iter.hasNext()) {
						//context.write(new Text(iter.next() + fieldDelimiter + (++sequence)), NullWritable.get());
						context.write(NullWritable.get(), new Text(iter.next() + fieldDelimiter + (++sequence)));
					}				
				}						
			}
			else {//竖表，没有序号
				if(reserveFieldIdxArrList != null){
					while(iter.hasNext()){			
						String rcd = iter.next();
						String[] fields ;
						if(fieldDelimiter.equals("|")){
							fields = rcd.split("\\|",-1);
						}else {
							fields = rcd.split(fieldDelimiter);
						}
						if(reserveFieldIdxArrList != null){
							String result = fields[reserveFieldIdxArrList.get(0)];
							for(int i = 1; i < reserveFieldIdxArrList.size(); ++i){
								result += fieldDelimiter + fields[reserveFieldIdxArrList.get(i)];
							}
							//context.write(new Text(result), NullWritable.get());
							context.write(NullWritable.get(), new Text(result));	
						}
					}
				}else {
					while(iter.hasNext()){						
						//context.write(new Text(iter.next()), NullWritable.get());	
						context.write(NullWritable.get(), new Text(iter.next()));
					}
				}			
			}
		}else {//横表
			String result = key.toString();
			if(reserveFieldIdxArrList != null){
				while(iter.hasNext()){			
					String rcd = iter.next();
					String[] fields ;
					if(fieldDelimiter.equals("|")){
						fields = rcd.split("\\|",-1);
					}else {
						fields = rcd.split(fieldDelimiter);
					}				
					for(int i = 0; i < reserveFieldIdxArrList.size(); ++i){
						result += fieldDelimiter + fields[reserveFieldIdxArrList.get(i)];
					}						
				}
				//context.write(new Text(result),NullWritable.get());
				context.write(NullWritable.get(), new Text(result));
			}else {				
				while(iter.hasNext()){			
					String rcd = iter.next();
					String[] fields ;
					if(fieldDelimiter.equals("|")){
						fields = rcd.split("\\|",-1);
					}else {
						fields = rcd.split(fieldDelimiter);
					}				
					for(int i = 0; i < fields.length ; ++i){
						if(!keyFieldIdxArrList.contains(i)) {
							result += fieldDelimiter + fields[i];
						}
					}
				}
				//context.write(new Text(result),NullWritable.get());
				context.write(NullWritable.get(), new Text(result));				
			}	
		}		
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter","|");
		topNNumber = conf.getInt("top.number", 1);
		lowBound = conf.getInt("low.bound", 1);
		String orderModeStr = conf.get("order.mode");/*2:asc,6:asc*/		
		String[] modeArr= orderModeStr.split(",");
		for(String mode: modeArr){
			String[] split = mode.split(":");
			if(split.length == 2){
				orderMode.put(Integer.parseInt(split[0]), split[1]);
			}
		}		
		
		//输出格式相关
		withSequence = conf.getBoolean("with.sequence", true);
		isVertical = conf.getBoolean("is.vertical", true);
		
		String reserveFieldIdxStr = conf.get("reserve.field.indexes","");/*以逗号分隔*/		
		if(!"".equals(reserveFieldIdxStr)){
			reserveFieldIdxArrList = new ArrayList<Integer>();
			String[] tmp = reserveFieldIdxStr.split(",");
			for (int i = 0; i < tmp.length; i++) {
				reserveFieldIdxArrList.add(Integer.parseInt(tmp[i]));
			}			
		}
		if(!isVertical && reserveFieldIdxArrList == null){
			keyFieldIdxArrList  = new ArrayList<Integer>();
			String[] keyFieldIdxArr = conf.get("key.field.indexes","0").split(",");
			for (int i = 0; i < keyFieldIdxArr.length; i++) {
				keyFieldIdxArrList.add(Integer.parseInt(keyFieldIdxArr[i]));			
			}
		}		
	}
}
