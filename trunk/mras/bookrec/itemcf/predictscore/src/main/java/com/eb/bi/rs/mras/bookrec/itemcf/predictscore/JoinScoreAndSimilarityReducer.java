package com.eb.bi.rs.mras.bookrec.itemcf.predictscore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinScoreAndSimilarityReducer extends Reducer<TextPair, Text, Text, NullWritable>{
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		HashMap<String, String> simMap = new HashMap<String, String>();
		String userID, score, desBookid, simlarity;
		for(Text value:values){
			String[] fields = value.toString().split("\\|");
			//0|目的图书ID|相似度
			if(fields[0].equals("0")){
				simMap.put(fields[1], fields[2]);
				continue;
			}
			
			//0|用户ID|评分
			if(fields[0].equals("1")){
				userID = fields[1];
				score = fields[2];
				Iterator<String> iterator = simMap.keySet().iterator();
				while(iterator.hasNext()) {
					desBookid = iterator.next();
					simlarity = simMap.get(desBookid);
					// 用户ID|源图书ID|评分|目的图书ID|相似度
					context.write(new Text(userID + "|" + key.getFirst() + "|" + score + "|" + desBookid + "|" + simlarity), NullWritable.get());
				}
			}
		}
	}
}

