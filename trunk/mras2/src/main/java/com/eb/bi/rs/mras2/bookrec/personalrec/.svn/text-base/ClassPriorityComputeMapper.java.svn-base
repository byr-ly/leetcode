package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassPriorityComputeMapper extends Mapper<Text, Text, Text, Text>{
	private String dataformatType;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		dataformatType = context.getConfiguration().get("Appconf.data.format.type");
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		if(dataformatType.equals("s")){
			context.write(key,new Text(value));
		}
		else{
			String[] fields = value.toString().split("\\|");
			
			for(int i = 0;i != fields.length;i++){
				context.write(key,new Text(fields[i]));
			}
		}
	}
}
