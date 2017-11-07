package com.eb.bi.rs.mras.bookrec.corelationrec.OrderFilterRead;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class OrderRecReultFilterReducer extends Reducer<Text, Text, Text, NullWritable>{
	
   private int orderCorelationRecNum;
   private int orderfilterreadtopN;
   private HashSet<String> readRecBookSet = new HashSet<String>();
   private String orderRecResult =null;
   private boolean flag = false;
   protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
     this.readRecBookSet.clear();
     flag=false;
     for (Text value : values){
       String[] fields = value.toString().split("\\|", -1);
       if (fields[0].contains("A")){
    	 if(fields.length > 50){
    		 for (int i = 2; i <= 2 * this.orderfilterreadtopN; i = i+2) {//取阅读还阅读的推荐结果top20
    	           this.readRecBookSet.add(fields[i]);
    	     }
    	 } 
       }else if (fields[0].contains("B")){
    	 flag=true;
         this.orderRecResult = value.toString();
       }
     }
     if (!flag) {
       return;
     }
     StringBuffer sb = new StringBuffer(key.toString());
     String[] buffer = this.orderRecResult.split("\\|", -1);
     int count = 0;
     for (int i = 2; i < buffer.length-1; i += 2) {
       if (!this.readRecBookSet.contains(buffer[i])){
         if (count >= this.orderCorelationRecNum - this.orderfilterreadtopN) {
           break;
         }
         sb.append("|" + buffer[i] + "|" + buffer[(i + 1)]);
         count++;
       }
     }
     context.write(new Text(sb.toString()), NullWritable.get());
   }
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException {  
	 Configuration conf = context.getConfiguration();
	 this.orderCorelationRecNum = conf.getInt("order.corelation.recommend.number", 70);
	 this.orderfilterreadtopN = conf.getInt("order.filter.read.topN", 20);    
   }
 }

