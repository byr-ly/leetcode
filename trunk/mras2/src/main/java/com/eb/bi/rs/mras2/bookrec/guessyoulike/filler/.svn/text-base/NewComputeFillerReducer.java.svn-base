package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NewComputeFillerReducer extends Reducer<Text,Text, NullWritable, Text> {
	private int recommendMim = 0;
	private int recommendMax = 0;
	
	private String SortWay = "";
	
	private MultipleOutputs<NullWritable,Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		//初始化多输出
		mos = new MultipleOutputs<NullWritable,Text>(context);
		
		//补白下限
		recommendMim = Integer.valueOf(context.getConfiguration().get("Appconf.filler.minnum"));
		//补白上限
		recommendMax = Integer.valueOf(context.getConfiguration().get("Appconf.filler.maxnum"));
	
		SortWay = context.getConfiguration().get("Appconf.filler.sortway");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//用户推荐结果存储
		List<DataStore> recommendList = new ArrayList<DataStore>();
		//已补白的图书存储
		List<DataStore> fillerList = new ArrayList<DataStore>();
		
		//已推图书列表
		Set<String> bookSet = new HashSet<String>();
		//用户阅读历史列表
		Set<String> hisSet = new HashSet<String>();
		//补白列表
		List<DataStore> whiteList = new ArrayList<DataStore>();
		
		//补白后输出结果
		//List<DataStore> outputresult = new ArrayList<DataStore>();
		
		//数据分离
		for(Text value:values){
			String[] fields = value.toString().split("\\|");
			
			//用户推荐结果//0|用户|图书|来源|分
			if(fields[0].equals("0")){
				recommendList.add(new DataStore(fields[2],value.toString().substring(2),Float.valueOf(fields[4])));
				bookSet.add(fields[2]);
				continue;
			}
			//用户阅读历史//1|用户|图书
			if(fields[0].equals("1")){
				hisSet.add(fields[2]);
				continue;
			}
			//用户可补白图书//2|用户|图书|来源(0)|分
			if(fields[0].equals("2")){
				whiteList.add(new DataStore(fields[2],value.toString().substring(2),Float.valueOf(fields[4])));
				continue;
			}
		}
		
		//改用没有推荐信息
		if(recommendList.size()==0){
			return;
		}
		
		
		ComparatorBook comparator=new ComparatorBook();
		Collections.sort(recommendList, comparator);
		//判断推荐结果是否需要补白
		if(bookSet.size()==recommendMax){//无需补白直接输出
			//补白输出
			outputfunc(recommendList,key.toString());
//			bookSet.clear();
//			hisSet.clear();
//			recommendList.clear();
//			whiteList.clear();
			return;
		}
		
		//补白库排序
		if(!(whiteList.size()==0)){
			Collections.sort(whiteList, comparator);
		}
		
		//图书补白
		for(int i = 0; i != whiteList.size(); i++){
			if(hisSet.contains(whiteList.get(i).getKey())
					||bookSet.contains(whiteList.get(i).getKey())){
				continue;
			}
			
			fillerList.add(whiteList.get(i));
			
			//补白成功
			bookSet.add(whiteList.get(i).getKey());
			//补白完成
			if(bookSet.size()==recommendMax){
				break;
			}
		}
		
		//是否补白成功
		if(bookSet.size()<recommendMim){//补白失败
			return;
		}
		
		//补白输出
		outputfunc(sortwayfunc(SortWay,recommendList,fillerList),key.toString());
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		mos.close();
	}
	
	public void outputfunc(List<DataStore> result,String key) throws IOException, InterruptedException{
		//结果按分排序输出
		//待添加
//		ComparatorBook comparator=new ComparatorBook();
//		Collections.sort(result, comparator);
		//------------
		
		//横表结果
		String resultString = key + "|";
		
		//竖表输出
		for(int i = 0;i != result.size();i++){
			//context.write(NullWritable.get(),new Text(result.get(i).toString()));
			mos.write(NullWritable.get(), new Text(result.get(i).toString()), "hive/part");
			
			//拼接
			//用户|图书|来源|分
			String[] fields = result.get(i).toString().split("\\|");
			resultString = resultString + fields[1] + ",|";
		}
		
		//横表输出
		resultString = resultString.substring(0, resultString.length()-1);
		//context.write(NullWritable.get(),new Text(resultString));
		mos.write(NullWritable.get(), new Text(resultString), "hbase/part");
	}
	
	public class ComparatorBook implements Comparator{
		public int compare(Object arg0, Object arg1){
			DataStore book0=(DataStore)arg0;
			DataStore book1=(DataStore)arg1;

			int flag;
			if(book0.getVal()>book1.getVal()){
				flag=-1;
				return flag;
			} else if (book0.getVal()==book1.getVal()){
				flag=0;
			} else {
				flag=1;
				return flag;
			}
			
			if(flag==0){
				return book0.getKey().compareTo(book1.getKey());
			} else {
				return flag;
			}
		}
	}
	
	public List<DataStore> sortwayfunc(String way,List<DataStore> recommed,List<DataStore> filler){
		//补白后排序输出规则
		List<DataStore> outputresult = new ArrayList<DataStore>();
		
		if(way.equals("1")){//顺序输出
			outputresult.addAll(recommed);
			outputresult.addAll(filler);
		}
		else if(way.equals("2")){//推荐，补白组内有序，组间随机
			int i=0;
			int j=0;
			while(true){
				if(i==recommed.size()){
					for(;j!=filler.size();j++){
						outputresult.add(filler.get(j));
					}
					break;
				}
				
				if(j==filler.size()){
					for(;i!=recommed.size();i++){
						outputresult.add(recommed.get(i));
					}
					break;
				}
				
				int rd=Math.random()>0.5?1:0;
				
				if(rd==0){
					outputresult.add(recommed.get(i));
					i++;
				} else {
					outputresult.add(filler.get(j));
					j++;
				}
			}
		} else {
			
		}
		return outputresult;
	}
}
