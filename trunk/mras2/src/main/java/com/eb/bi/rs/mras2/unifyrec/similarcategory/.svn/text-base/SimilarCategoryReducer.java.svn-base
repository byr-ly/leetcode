package com.eb.bi.rs.mras2.unifyrec.similarcategory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SimilarCategoryReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	 private String result=new String();
	
	 protected void reduce(Text key, Iterable<Text> values, Context context) throws 
	 IOException, InterruptedException {
           
		 StringBuffer idScore=new StringBuffer();
		 for (Text val : values) {  
			 idScore.append(val.toString());
			 idScore.append(",");
			}  
		 idScore.deleteCharAt(idScore.length() - 1);
		 result=key+"_"+idScore;
		 
		 context.write(new Text(result.toString()), NullWritable.get());
		 }
		 
	 
		
	 
	 
}
