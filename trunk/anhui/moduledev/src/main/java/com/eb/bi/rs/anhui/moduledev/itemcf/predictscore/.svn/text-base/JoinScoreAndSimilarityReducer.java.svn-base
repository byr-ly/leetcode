package com.eb.bi.rs.anhui.moduledev.itemcf.predictscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinScoreAndSimilarityReducer extends Reducer<Text, Text, Text, NullWritable>{
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		
		HashMap<String, String> simMap = new HashMap<String, String>();
		HashMap<String, String> userMap = new HashMap<String, String>();
		simMap.clear();userMap.clear();
		String userId, desBrandid=null;
		for(Text value:values){
			String[] fields = value.toString().split("\\|");
			//1|目的品牌ID|相似度
			if(fields[0].equals("1")){
				simMap.put(fields[1], fields[2]);
			}else if(fields[0].equals("0")){
				//0|用户ID|评分
				userMap.put(fields[1], fields[2]);
			}
		}
		Iterator<String> userIterator = userMap.keySet().iterator();
		while(userIterator.hasNext()) {
			userId = userIterator.next();
			Double oraScore  = Double.parseDouble(userMap.get(userId));
			Iterator<String> simIterator = simMap.keySet().iterator();
			while(simIterator.hasNext()){//对于每一个目的品牌
				desBrandid = simIterator.next();
				Double similarity =  Double.parseDouble(simMap.get(desBrandid));
				// 用户ID|源图书ID|目的图书ID|目的图书预测分
				context.write(new Text(userId + "|" + desBrandid + "|" + similarity*oraScore), NullWritable.get());
			}
		}
	}
	
}

