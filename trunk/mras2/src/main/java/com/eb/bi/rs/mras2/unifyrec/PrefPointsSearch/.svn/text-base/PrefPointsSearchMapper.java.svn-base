package com.eb.bi.rs.mras2.unifyrec.PrefPointsSearch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PrefPointsSearchMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String searchData;
	 
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		searchData=conf.get("conf.search.data");  //13003917520|,13003917113|
	}
	
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		 String line = value.toString();
		 if(searchData!=null&&!searchData.equals("")){
			 String [] userId=searchData.split("\\,",-1); //13003917520|
			 if(userId.length>0){
				 for(int i = 0; i < userId.length; i++){
	                if(line.contains(userId[i])){
	                    context.write(new Text(userId[i]), new Text(line));
	                }
			     }
		     
			 }
		 }
	 }	 
}

