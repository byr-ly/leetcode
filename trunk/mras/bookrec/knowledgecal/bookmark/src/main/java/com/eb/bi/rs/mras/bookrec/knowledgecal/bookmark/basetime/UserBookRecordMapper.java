package com.eb.bi.rs.mras.bookrec.knowledgecal.bookmark.basetime;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserBookRecordMapper extends Mapper<Object, Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		//用户|图书|最后一次访问时间YYYYMMDD
		String[] fields = value.toString().split("\\|",-1);
		
		if(fields.length!=3){
			return;
		}
		
		if(fields[0].equals("")||fields[1].equals("")||fields[2].equals(""))
			return;
		
		context.write(new Text(fields[0]+"|"+fields[1]),new Text("1|"+fields[2]));
	}
}
