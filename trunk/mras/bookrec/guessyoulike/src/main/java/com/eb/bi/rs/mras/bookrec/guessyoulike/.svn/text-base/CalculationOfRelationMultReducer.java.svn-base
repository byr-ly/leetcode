package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.Similar;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.Similars;
import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;

public class CalculationOfRelationMultReducer extends Reducer<TextPair,Text, Text, Text> {
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		Similars similars = new Similars();
		for(Text value:values){
			//System.out.println(value.toString());
			
			String[] fields = value.toString().split("\\|");
			//对于一本源图书的相似度加载
			if(fields[0].equals("0")){
				//0|图书|相似度向量
				similars.add(fields[1], fields[2], ",");
				
				//System.out.println(similars.getSimilars().size());
				
				continue;
			}
			//System.out.println(similars.getSimilars().size());
			
			//else:对于一本源图书,用户偏好的计算
			//1|用户|图书打分|来源集
			//用户
			String userId = fields[1];
			//图书打分
			float sbookScore = Float.valueOf(fields[2]);
			//来源集
			String[] sources = fields[3].toString().split(",");
			
			Iterator<Entry<String, Similar>> it = similars.getSimilars().entrySet().iterator();
			while (it.hasNext()){
				Map.Entry<String, Similar> entry = (Map.Entry<String, Similar>) it.next();
				String pbook;
				pbook = entry.getKey();//待推荐图书
				Similar pSimilar = new Similar();
				pSimilar = entry.getValue();//相似度向量
				
				pSimilar.similarMult(sbookScore);
				
				String similarString = "";
				similarString = pSimilar.tosimilarString(",");
				
				//System.out.println(pbook+"|"+sbookScore+"|"+similarString);
				
				for(int j =0; j != sources.length; j++){
					//key:用户|待推荐图书|源图书来源;val:源图书|计算后相似度向量
					String[] sbook = key.getFirst().toString().split("\\|");
					
					context.write(new Text(userId+"|"+pbook+"|"+sources[j]),new Text(sbook[0]+"|"+similarString));
				}
			}
		}
		//System.out.println("one key:"+key.toString());
	}
}
