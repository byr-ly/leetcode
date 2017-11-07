package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EntropyReducer extends Reducer<Text, Text, Text, Text> {
 
	
	private double[] indicatorEntropy;
	//private double[] indicatorWeight;
	private int indicatorCount;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	  super.setup(context);	  

	  indicatorCount = Integer.parseInt(context.getConfiguration().get("indicatorCount")); 
	  indicatorEntropy = new double[indicatorCount];
	  for(int idx = 0; idx < indicatorCount; ++idx){		  
		  indicatorEntropy[idx] = 0.0;
	  }
//	  indicatorWeight = new double[indicatorCount];
//	  for(int idx = 0; idx < indicatorCount; ++idx){		  
//		  indicatorWeight[idx] = 0.0;
//	  }  

	}
	
	@Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
  	  	
        for (Text value : values) { 
        	String[] fields = value.toString().split(":");      	
        	for(int idx = 0; idx < indicatorCount; ++idx){
        		indicatorEntropy[idx] += Double.parseDouble(fields[idx]);
        	}
        }
        
//        for(int idx = 0; idx < indicatorCount; ++idx){
//			System.out.println(String.valueOf(indicatorEntropy[idx]));
//		}
//       
//		
//		double sum = 0.0;		 
//		for(int idx = 0; idx < indicatorCount; ++idx){
//			sum += 1 - indicatorEntropy[idx];
//		}
//		for(int idx = 0; idx < indicatorCount; ++idx){
//			indicatorWeight[idx] = (1 - indicatorEntropy[idx])/ sum;
//		}        
        
        String writeString = "";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString += String.valueOf(indicatorEntropy[idx]) + ":";
		}
		context.write(key, new Text(writeString.substring(0, writeString.length()-1)));	
    }   	

  }
