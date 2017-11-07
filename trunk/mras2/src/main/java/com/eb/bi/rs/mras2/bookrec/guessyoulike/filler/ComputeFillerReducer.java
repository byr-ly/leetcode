package com.eb.bi.rs.mras2.bookrec.guessyoulike.filler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class ComputeFillerReducer extends Reducer<Text,Text, NullWritable, Text> {
	private List<DataStore> fillterList = new ArrayList<DataStore>();
	private int recommendMim = 0;
	private int recommendMax = 0;
	private MultipleOutputs<NullWritable,Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		//初始化多输出
		mos = new MultipleOutputs<NullWritable,Text>(context);
		///加载补白库文件
		//补白下限
		recommendMim = Integer.valueOf(context.getConfiguration().get("Appconf.filler.minnum"));
		//补白上限
		recommendMax = Integer.valueOf(context.getConfiguration().get("Appconf.filler.maxnum"));
		
		super.setup(context);
		URI[] localFiles = context.getCacheFiles();		
		for(URI localFile : localFiles){
			String line;
			BufferedReader in = null;
			try {
                Path path = new Path(localFile.getPath());
                in = new BufferedReader(new FileReader(path.getName().toString()));
                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");

                    if (fields.length != 1) {
                        fillterList.add(new DataStore(fields[0], fields[0], Float.valueOf(fields[1])));
                    } else {
                        fillterList.add(new DataStore(fields[0], fields[0], 0));
                    }
                }
            } finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
		//将补白库按分排序
		//待添加
		//------------
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//待补白结果存储
		List<DataStore> result = new ArrayList<DataStore>();
		//黑名单
		Set<String> blackList = new HashSet<String>();
		
		//已推图书
		Set<String> bookSet = new HashSet<String>();
		
		//黑名单，推荐结果分离
		for(Text value:values){
			String[] fields = value.toString().split("\\|");
			
			if(fields[0].equals("0")){
				//0|用户|图书|来源|分
				bookSet.add(fields[2]);
				result.add(new DataStore(fields[2],fields[1]+"|"+fields[2]+"|"+fields[3]+"|"+fields[4],Float.valueOf(fields[4])));//用户|图书|来源|分
			} else{
				//黑名单:1|用户|图书
				blackList.add(fields[2]);//图书
			}
		}
		
		//用户没有推荐结果
		if(result.size()==0)
			return;
		
		//无需补白用户结果直接输出
		if(result.size()>=recommendMim){
			outputfunc(result,key.toString());
			return;
		}
		
		///补白操作
		//补白输出循环
		for (int i = 0;i != 500;i++){//DataStore oneBook:fillterList
			//随机一个索引号
			int random = new Random().nextInt(fillterList.size());
			//随机取出补白图书
			DataStore oneBook = fillterList.get(random);
			
			//是否属于黑名单
			if(blackList.contains(oneBook.getKey())){
				continue;
			}
			
			//是否已推
			if(bookSet.contains(oneBook.getKey())){
				continue;
			}
			
			///add
			//已推列表
			bookSet.add(oneBook.getKey());
			//用户|图书|来源|分
			result.add(new DataStore(oneBook.getKey(),key.toString()+"|"+oneBook.getKey()+"|"+0+"|"+0,0));
			
			//补白完成
			if(result.size()==recommendMax){
				break;
			}
		}
		
		//补白后个数大于等于4,则推荐(补白成功)
		if(result.size()>=recommendMim){
			outputfunc(result,key.toString());
		}
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		mos.close();
	}
	
	public void outputfunc(List<DataStore> result,String key) throws IOException, InterruptedException{
		//结果按分排序输出
		//待添加
		ComparatorBook comparator=new ComparatorBook();
		Collections.sort(result, comparator);
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
}
