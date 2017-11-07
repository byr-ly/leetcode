package com.eb.bi.rs.mras2.bookrec.personalrec.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NewBookFillerReducer extends Reducer<Text,Text, Text, Text> {
	private List<BookStore> m_BookList = new ArrayList<BookStore>();
	
	private int m_FillerNum;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_FillerNum = Integer.valueOf(context.getConfiguration().get("Appconf.filler.num"));
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		List<String> DataList = new ArrayList<String>();
		
		for(Text value:values){
			DataList.add(value.toString());
		}
		
		for(int i = 0; i != DataList.size(); i++){
			//m_BookList.add(new BookStore(DataList.get(i)));
		}
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException {
		
	}
}
