package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndicatorFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private int deepReadThreshold;
	private Map<String, Integer> bookDeepReadFrequencyMap = new HashMap<String, Integer>();	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype 
		String[] fields = value.toString().split("\\|",-1);
		if(fields.length >= 10){
			//B书的深度阅读用户数大于一定阈值
			String book2 = fields[1];
			if(bookDeepReadFrequencyMap.containsKey(book2)&&bookDeepReadFrequencyMap.get(book2) > deepReadThreshold){
					context.write(value, NullWritable.get());						
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException 	{
		
		Configuration conf = context.getConfiguration();
		deepReadThreshold = conf.getInt("deep.read.threshold", 50);
		String bookDeepReadFrequencyPath = conf.get("book.click.path");
		bookDeepReadFrequencyMap.clear();
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {			
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if (localFiles[i].toString().contains(bookDeepReadFrequencyPath)) {
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						if(fields.length >= 5) {
							bookDeepReadFrequencyMap.put(fields[0], Integer.parseInt(fields[4]));
						}					
					}					
				} 
			} finally{
				if(in != null){
					in.close();
				}				
			}		
			
		}
		
	}

}
