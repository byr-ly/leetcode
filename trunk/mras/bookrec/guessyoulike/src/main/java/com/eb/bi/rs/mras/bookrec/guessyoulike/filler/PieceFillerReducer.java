package com.eb.bi.rs.mras.bookrec.guessyoulike.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PieceFillerReducer extends Reducer<Text,Text, NullWritable, Text> {
	//private List<DataStore> fillterList = new ArrayList<DataStore>();
	
	//事业部id,booklist
	private Map<String,ArrayList<String>> books = new HashMap<String,ArrayList<String>>();
	
	private int recommendNum = 0;
	private int randomNum = 0;
	
	private String Ways = "";
	
	private String type = "";
	
	@Override
	protected void setup(Context context
			) throws IOException,InterruptedException {
		recommendNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.recommendnum"));
		randomNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.randomnum"));
	
		Ways = context.getConfiguration().get("Appconf.random.way");
		type = context.getConfiguration().get("Appconf.book.type");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context
			) throws IOException, InterruptedException{
		ArrayList<String> bookList = new ArrayList<String>();
		
		//事业部id|图书
		for(Text value:values){
			//fillterList.add(new DataStore(key.toString(),value.toString(),0));
			
			bookList.add(value.toString());
		}
		
		books.put(key.toString(), bookList);
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		//解析规则
		String[] WaysFields = Ways.toString().split(";");
		
		//分设备生成补白结果
		for(int i=0;i!=WaysFields.length;i++){
			String[] oneway = WaysFields[i].toString().split(",");
			
			for(int j=0;j!=randomNum;j++){
				ArrayList<String> resultList = selectFunc(WaysFields[i],type);
				
				String resultString = "";
				Iterator<String> it = resultList.iterator();
				while (it.hasNext()){
					String entry = it.next();
					
					if(entry.equals("")){
						continue;
					}
					
					//图书,理由(空)|图书,理由(空)|...
					resultString = resultString + entry + ",|";
				}
				resultString = resultString.substring(0, resultString.length()-1);
				//写结果
				if(i==0){
					context.write(NullWritable.get(),new Text(j+"|"+resultString));
				}
				
				context.write(NullWritable.get(),new Text(j+"_"+oneway[0]+"|"+resultString));
			}
		}
	}
	
	//随机选书
	public ArrayList<String> selectFunc(String oneWay, String type){
		Set<String> resultSet = new HashSet<String>();
		ArrayList<String> resultList = new ArrayList<String>(recommendNum);
		
		for(int n=0;n!=recommendNum;n++){
			resultList.add("");
		}
		
		Map<String,Integer> typeAnum = new HashMap<String,Integer>();
		
		//规则处理
		String[] typeFields = type.toString().split(",");
		String[] WaysFields = oneWay.toString().split(",");
		for(int i=0;i!=typeFields.length;i++){
			typeAnum.put(typeFields[i], Integer.valueOf(WaysFields[i+1]));
		}
		
		Iterator<Entry<String, ArrayList<String>>> iter = books.entrySet().iterator();
		while (iter.hasNext()){
			Map.Entry<String, ArrayList<String>> entry = (Map.Entry<String, ArrayList<String>>) iter.next();
			String key = entry.getKey();
			ArrayList<String> val = new ArrayList<String>(entry.getValue());
			
			int typeNum=typeAnum.get(key);
			
			int count=0;
			
			for(;;){
				int random = new Random().nextInt(val.size());
				
				if(resultSet.contains(val.get(random))){
					continue;
				}
				else{
					resultSet.add(val.get(random));
					
					int index=0;
					
					index=(recommendNum/typeNum)*count;
					
					for(;;){
						if(index>=recommendNum){
							index=0;
						}
						if(resultList.get(index).equals("")){
							resultList.set(index, val.get(random));
							break;
						}
						else{
							index++;
						}
					}
					count++;
				}
				
				if(count==typeNum){
					break;
				}
				
				if(count==val.size()){
					break;
				}
			}
		}
		
		if(resultSet.size()<recommendNum){
			Iterator<Entry<String, ArrayList<String>>> iter2 = books.entrySet().iterator();
			while (iter2.hasNext()){
				Map.Entry<String, ArrayList<String>> entry = (Map.Entry<String, ArrayList<String>>) iter2.next();
				String key = entry.getKey();
				ArrayList<String> val = new ArrayList<String>(entry.getValue());
			
				for(int k=0;k!=val.size();k++){
					if(!resultSet.contains(val.get(k))){
						resultSet.add(val.get(k));
						for(int l=0;l!=recommendNum;l++){
							if(resultList.get(l).equals("")){
								resultList.set(l, val.get(k));
								break;
							}
						}
					}
					if(resultSet.size()==recommendNum){
						break;
					}
				}
				if(resultSet.size()==recommendNum){
					break;
				}
			}
		}
		
		return resultList;
	}
	
/*	
	
	public int HashFunc(int fillernum, int typenum, int count){
		static Set<String> numlist = new HashSet<String>();
		
		
		return 0;
	}
*/
	
	
	/*
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		///随机选取补白图书
		//随机randomNum套方案
		for(int i = 0; i != randomNum;i++){
			Set<String> resultSet = new HashSet<String>();			
			//每套方案随机recommendNum本
			for(;;){
				int random = new Random().nextInt(fillterList.size());
				
				resultSet.add(fillterList.get(random).getKey());
				
				if(resultSet.size()==recommendNum){
					break;
				}
			}
			//结果拼接
			String resultString = "";
			Iterator<String> it = resultSet.iterator();
			while (it.hasNext()){
				String entry = it.next();
				//图书,理由(空)|图书,理由(空)|...
				resultString = resultString + entry + ",|";
			}
			resultString = resultString.substring(0, resultString.length()-1);
			//写结果
			context.write(NullWritable.get(),new Text(i+"|"+resultString));
			resultSet.clear();
		}
		fillterList.clear();
	}
	*/
}
