package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ServiceTypeReducer extends Reducer<Text, Text, Text, Text> {
 
	private double[] maxIndicators;
	private double[] minIndicators;
	private double[] sumIndicators;
	private long rcdCount;

	private int indicatorCount;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	  super.setup(context);
	  
	  rcdCount = 0;
	  indicatorCount = Integer.parseInt(context.getConfiguration().get("indicatorCount")); 
	  maxIndicators = new double[indicatorCount];
	  minIndicators = new double[indicatorCount];
	  sumIndicators = new double[indicatorCount];
	  for(int idx = 0; idx < indicatorCount; ++idx){		  
		  maxIndicators[idx] = Double.MIN_VALUE;
		  minIndicators[idx] = Double.MAX_VALUE;
		  sumIndicators[idx] = 0.0;
	  }
	 

	}
	
	@Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
  	  	
        for (Text value : values) { 
        	System.out.println("value =" + value.toString());
        	String[] fields = value.toString().split(":");
        	/*compute max values of indicators*/
        	if(fields[0].equals("maxIndicators")){
            	for(int idx = 0; idx < indicatorCount; ++idx){
	        		double tmp = Double.parseDouble(fields[idx + 1]);
					if( tmp > maxIndicators[idx]){
						maxIndicators[idx] = tmp;	
					}
            	}			
    		}
	    	/*compute min values of indicators*/
	    	else if(fields[0].equals("minIndicators")){	    		
	            	for(int idx = 0; idx < indicatorCount; ++idx){
		        		double tmp = Double.parseDouble(fields[idx + 1]);
						if( tmp < minIndicators[idx]){
							minIndicators[idx] = tmp;	
						}
	            	}				
	    	}
	    	else if(fields[0].equals("sumIndicators")) {
	    		for(int idx = 0; idx < indicatorCount; ++idx){
	        		double tmp = Double.parseDouble(fields[idx + 1]);
					sumIndicators[idx] += tmp;
            	}		
	    		
	    	}      	
	    	else if(fields[0].equals("rcdCount")){
	    		rcdCount += Long.parseLong(fields[1].toString());
	    	}
        }
		String writeString = "maxIndicators:";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString += String.valueOf(maxIndicators[idx]) + ":";
		}
		context.write(key, new Text(writeString.substring(0, writeString.length()-1)));	
		
		writeString = "minIndicators:";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString +=  String.valueOf(minIndicators[idx]) + ":"  ;
		}		
		context.write(key, new Text(writeString.substring(0, writeString.length()-1)));	
		
		writeString = "sumIndicators:";
		for(int idx = 0; idx < indicatorCount; ++idx){
			writeString +=  String.valueOf(sumIndicators[idx]) + ":"  ;
		}		
		context.write(key, new Text(writeString.substring(0, writeString.length()-1)));	
		
		context.write(key, new Text("rcdCount:" + String.valueOf(rcdCount)));
    }   	

  }
