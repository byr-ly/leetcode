package com.eb.bi.rs.mras.bookrec.personalrec.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HotbookFillerReducer extends Reducer<Text,Text, NullWritable, Text> {
	//key:bu_type;val:book_id,class_id,real_fee
	private Map<String,BookStores> m_BookMap = new HashMap<String,BookStores>();
	
	private int m_FillerNum = 0;
	
	private int randomNum = 0;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_FillerNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.recommendnum"));
		randomNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.randomnum"));
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		List<String> DataList = new ArrayList<String>();
		
		for(Text value:values){
			DataList.add(value.toString());
		}
		
		//如果分类下图书小于30本，该分类不作为补白分类
		if(DataList.size()<30)
			return;
		
		for(int i = 0; i != DataList.size(); i++){
			//bookid图书|bu_type事业部|class_id分类|real_fee总费用
			String[] fields = DataList.get(i).split("\\|");
			
			if(m_BookMap.containsKey(fields[1])){
				
			}
			else{
				BookStores bookstores = new BookStores();
				bookstores.add(fields[2], fields[0], fields[1]);
				m_BookMap.put(fields[1], bookstores);
			}
			//m_BookMap.put(fields[1],new BookStores(DataList.get(i)));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		
		//Collections.sort(m_BookList, new BookStore());
		/*
		for(int j = 0; j != randomNum; j++){
			for(int i = 0; i != m_BookList.size(); i++){
			
			}
			context.write(NullWritable.get(),new Text("|0"));
		}*/
	}
}
