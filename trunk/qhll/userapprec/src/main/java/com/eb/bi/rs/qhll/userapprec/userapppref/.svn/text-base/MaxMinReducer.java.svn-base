package com.eb.bi.rs.qhll.userapprec.userapppref;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinReducer extends Reducer<Text, Text, Text, Text> {
    private final Text writeKey = new Text("");
    
    private double[] maxIndicators;
	private double[] minIndicators;
	private int indicatorCount;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	  super.setup(context);

	  indicatorCount = Integer.parseInt(context.getConfiguration().get("indicatorCount"));;
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
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
  	  	
        for (Text value:values) {         	
        	String[] fields = value.toString().split(":");
        	for(int idx = 0; idx < indicatorCount; ++idx){
        		double tmpMax = Double.parseDouble(fields[2*idx]);
        		double tmpMin = Double.parseDouble(fields[2*idx + 1]);
        		if(tmpMax > maxIndicators[idx]){
        			maxIndicators[idx] = tmpMax;        			
        		}
        		if(tmpMax < minIndicators[idx]){
        			minIndicators[idx] = tmpMin;        			
        		}        		
        	}

        }
		String writeString = "";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString += String.valueOf(maxIndicators[idx]) + ":" + String.valueOf(minIndicators[idx]) + ":";
		}		
		context.write(writeKey, new Text(writeString.substring(0, writeString.length()-1)));	
    }
}