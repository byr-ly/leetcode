package com.eb.bi.rs.mras.bookrec.corelationrec.RealTimeFillerDataPrepare;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class RealTimeFillerFreeBookPrepareReducer extends Reducer<Text, Text, Text, NullWritable>{
	
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
  
	    StringBuffer result = new StringBuffer("freebook;");
	    
	    for(Text value : values){
	    	String field = value.toString();
			if(!field.equals("")) {
				result.append(field+"|");
			}
	    }
	    
	    context.write(new Text(result.toString()), NullWritable.get());
  }     
}


