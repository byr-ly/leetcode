package com.eb.bi.rs.mras2.bookrec.corelationrec.OrderFilterRead;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadRecResultMapper extends Mapper<Object, Text, Text, Text>{
	
   @Override
   protected void map(Object key, Text value, Context context)throws IOException, InterruptedException
   {
     String valueStr = value.toString();
     context.write(new Text(valueStr.split("\\|", -1)[0]), new Text("A|" + valueStr));
   }
 }