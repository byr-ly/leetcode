package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;





class MaxMinMapper extends Mapper<LongWritable, Text, Text, Text> {
	//private static final Logger log = Logger.getLogger(MaxMinMapper.class);
	private final Text writeKey = new Text("K");
	private double[] maxIndicators;
	private double[] minIndicators;
	private int indicatorCount;
	
	@Override	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {			
		
		
		String fields[] = value.toString().split("\\|");
		if(fields.length < 3 + indicatorCount){
			return;
		}
		for(int idx = 0; idx < indicatorCount; ++idx)
		{			
			double tmp  = Double.parseDouble(fields[idx + 3]);
			if( tmp > maxIndicators[idx]){
				maxIndicators[idx] = tmp;
			}
			if(	tmp < minIndicators[idx]){
				minIndicators[idx] = tmp;
			}
		}		
	}
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	  super.setup(context);
	 
	  
	  indicatorCount = Integer.parseInt(context.getConfiguration().get("indicatorCount"));
	  maxIndicators = new double[indicatorCount];
	  minIndicators = new double[indicatorCount];		
	  for(int idx = 0; idx < indicatorCount; ++idx){		  
		  maxIndicators[idx] = Double.MIN_VALUE;
	  }
	  for(int idx = 0; idx < indicatorCount; ++idx){		  
		  minIndicators[idx] = Double.MAX_VALUE;
	  }
	  
	 
	}
	@Override
	protected void cleanup(Context context) throws IOException,	InterruptedException {
		String writeString = "";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString += String.valueOf(maxIndicators[idx]) + ":" + String.valueOf(minIndicators[idx]) + ":";
		}		
		context.write(writeKey, new Text(writeString.substring(0, writeString.length()-1)));		
		super.cleanup(context);		
	}
}
