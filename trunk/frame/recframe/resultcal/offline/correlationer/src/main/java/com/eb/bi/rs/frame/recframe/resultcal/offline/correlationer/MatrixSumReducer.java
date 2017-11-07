package com.eb.bi.rs.frame.recframe.resultcal.offline.correlationer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MatrixSumReducer extends Reducer<Text,Text, Text, NullWritable> {
	private String ifhaveScore = "";
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		ifhaveScore = context.getConfiguration().get("Appconf.output.format.ifhavescore");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		float scoreSum = 0;
		
		for(Text value:values){
			scoreSum += Float.valueOf(value.toString());
		}
		
		//String[] keys = key.toString().split("\\???");//�ָ����
		if(ifhaveScore.equals("true")){
			context.write(new Text(key.toString()+"|"+scoreSum),NullWritable.get());
		}
		else{
			context.write(key,NullWritable.get());
		}
	}
}
