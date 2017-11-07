package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserBookSpointReducer extends Reducer<Text,Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		List<String> userreadbookList = new ArrayList<String>();
		
		//Set<String> bookSet = new HashSet<String>();
		
		//TYPE-章节数和
		Map<Integer, Float> TYPE_CHnum = new HashMap<Integer, Float>();
		//TYPE-图书数量
		Map<Integer, Integer> TYPE_Booknum = new HashMap<Integer, Integer>();
		
		//该用户的阅读图书总数量
		int book_TNum = 0;
		//该用户的阅读总章节数
		float CH_TNum = 0;//int CH_TNum = 0;
		
		int book_TYPE;
		
		for(Text value:values){
			//[0]图书id[1]用户阅读章节数[2]图书评分类型[3]图书主体得分
			String[] field = value.toString().split("\\|");
			
			//bookSet.add(field[0]);
			
			book_TNum++;
			
			CH_TNum+=Float.valueOf(field[1]);
			
			book_TYPE  = Integer.valueOf(field[2]);
			//满足该条件附加得分为0，直接得到结果
			if(book_TYPE==1||book_TYPE==5||book_TYPE==8||book_TYPE==9){
				context.write(key,new Text(field[0]+"|"+field[3]+"|"+0+"|"+field[3]));
			}
			else{
				userreadbookList.add(value.toString());
			}
//			System.out.println(book_TYPE+"|"+TYPE_CHnum.get(book_TYPE)+"|"+Float.valueOf(field[1]));
			
			float oldTYPE_CHnum = 0;
			if(TYPE_CHnum.get(book_TYPE)==null){oldTYPE_CHnum = 0;}
			else{oldTYPE_CHnum = TYPE_CHnum.get(book_TYPE);}
			
			TYPE_CHnum.put(book_TYPE, oldTYPE_CHnum+Float.valueOf(field[1]));
			
			int oldTYPE_Booknum = 0;
			if(TYPE_Booknum.get(book_TYPE)==null){oldTYPE_Booknum = 0;}
			else{oldTYPE_Booknum = TYPE_Booknum.get(book_TYPE);}
			
			TYPE_Booknum.put(book_TYPE, oldTYPE_Booknum+1);
		}
		
		//book_TNum = bookSet.size();
		
		float extra_score  = 0;
		float extra_score1 = 0;
		float extra_score2 = 0;
		
		float total_scroe = 0;
		
		//附加的分不为0的计算
		for(String readinfo:userreadbookList){
			//[0]图书id[1]用户阅读章节数[2]图书评分类型[3]图书主体得分
			String[] field = readinfo.toString().split("\\|");
			
			//实际计算部分------
			//System.out.println(book_TYPE+"|"+TYPE_CHnum.get(book_TYPE)+"|"+Float.valueOf(field[1]));
			extra_score1 =  (float) (
							Float.valueOf(field[1])/TYPE_CHnum.get(Integer.valueOf(field[2])) * 
							Math.log( Float.valueOf(field[1])/TYPE_CHnum.get(Integer.valueOf(field[2])) / (1.0/TYPE_Booknum.get(Integer.valueOf(field[2]))) )
							);
			
			extra_score2 = (float) (
							Float.valueOf(field[1])/CH_TNum * 
							Math.log( Float.valueOf(field[1])/CH_TNum / (1.0/book_TNum) )
							);
			//--------------
			
			if(extra_score1 < 0){
				extra_score1 = 0;
			}
			if(extra_score2 < 0){
				extra_score2 = 0;
			}
			extra_score = extra_score1 + extra_score2;
//			if(extra_score > 5){
//				extra_score = 5;
//			}
			total_scroe = Float.valueOf(field[3]) + extra_score;
			if(total_scroe > 5){
				total_scroe = 5;
			}
			
			context.write(key,new Text(field[0]+"|"+field[3]+"|"+extra_score+"|"+total_scroe));
		}
	}
}
