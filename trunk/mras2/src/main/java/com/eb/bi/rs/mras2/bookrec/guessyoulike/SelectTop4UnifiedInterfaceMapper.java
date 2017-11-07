package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eb.bi.rs.frame2.recframe.resultcal.offline.selector.util.StringDoublePair;

public class SelectTop4UnifiedInterfaceMapper extends Mapper<Object, Text, Text, StringDoublePair> {
	
	//private Map<String, TreeSet<StringDoublePair>> map  = new HashMap<String, TreeSet<StringDoublePair>>();
	//private int selectNumber;

	protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length == 4) {//用户|推荐图书|得分|版面集
			StringDoublePair pair = new StringDoublePair(fields[1] ,Double.parseDouble(fields[2]));
			String[] pages = fields[3].split(",");
			for (String page : pages) {							
				String outKey = fields[0] + "|" + page;//用户|版面
				context.write(new Text(outKey), pair);				
			}			
		}		
	}
	
//	protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
//		String[] fields = value.toString().split("\\|");
//		if (fields.length == 4) {//用户|推荐图书|得分|版面集
//			StringDoublePair pair = new StringDoublePair(fields[1] ,Double.parseDouble(fields[2]));
//			String[] pages = fields[3].split(",");
//			for (String page : pages) {							
//				String outKey = fields[0] + "|" + page;//用户|版面
//				if (map.containsKey(outKey)) {
//					TreeSet<StringDoublePair> set = map.get(outKey);
//					if (set.size() < selectNumber) {
//						set.add(pair);					
//					} else if (pair.compareTo(set.first()) > 0) {
//						set.remove(set.first());
//						set.add(pair);
//					}					
//				} else {
//					TreeSet<StringDoublePair> set = new TreeSet<StringDoublePair>();
//					set.add(pair);
//					map.put(outKey, set);
//					
//				}							
//			}			
//		}		
//	}
	
//	protected void setup(Context context) throws IOException ,InterruptedException {
//		selectNumber = context.getConfiguration().getInt("select.number.per.page", 80);
//	}
	
//	protected void cleanup(Context context) throws IOException ,InterruptedException {
//		Iterator<Entry<String, TreeSet<StringDoublePair>>> iter = map.entrySet().iterator();
//		while (iter.hasNext()) {
//			Entry<String, TreeSet<StringDoublePair>> item = iter.next();			
//			TreeSet<StringDoublePair> pairs = item.getValue();
//			for (StringDoublePair pair: pairs) {			
//				context.write(new Text(item.getKey()), pair);				
//			}			
//		}	
//	}

}