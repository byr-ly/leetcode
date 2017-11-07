package com.eb.bi.rs.mras2.cartoonrec.corelationrec.RealTimeFillerDataPrepare;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class RealTimeFillerDataPrepareMapper extends Mapper<Object, Text, Text, Text>{

  @Override
  protected void map(Object key, Text value, Context context)throws IOException, InterruptedException{
	
       context.write(new Text("1"), value);					
  }   
  
}


