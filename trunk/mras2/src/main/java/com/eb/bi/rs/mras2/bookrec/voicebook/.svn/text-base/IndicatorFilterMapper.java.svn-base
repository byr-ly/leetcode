package com.eb.bi.rs.mras2.bookrec.voicebook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndicatorFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private int deepReadThreshold;
	private Map<String, String> bookClassMap = new HashMap<String, String>();
	private Map<String, Integer> bookDeepReadFrequencyMap = new HashMap<String, Integer>();
	
	
	private Map<String, String> bookSeriesMap = new HashMap<String, String>();
	private Map<String, Set<String>> seriesBookSetMap = new HashMap<String, Set<String>>();			
	private Set<String> freeBookSet = new HashSet<String>();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype 
		String[] fields = value.toString().split("\\|",-1);
		assert fields.length == 10;
		if(fields.length == 10){			
			//A、B图书同属于一个大类；
			//B书的深度阅读用户数大于一定阈值
			String book1 = fields[0];
			String book2 = fields[1];
			if(bookClassMap.containsKey(book1) && bookClassMap.containsKey(book2) && bookDeepReadFrequencyMap.containsKey(book2)){//两个都没有下架
				String book1Class = bookClassMap.get(book1);
				String book2Class = bookClassMap.get(book2);
				//两本书属于同一个大类,且不为空
				if(book1Class.equals(book2Class) && !"".equals(book1Class) && bookDeepReadFrequencyMap.get(book2) > deepReadThreshold){
					//排除同系列
					if (bookSeriesMap.containsKey(book1)) {						
						Set<String> set = seriesBookSetMap.get(bookSeriesMap.get(book1));
						if (set.contains(book2)) {
							return;							
						}						
					}
					context.write(value, NullWritable.get());					
				}	
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException 	{
		
		Configuration conf = context.getConfiguration();
		deepReadThreshold = conf.getInt("deep.read.threshold", 10);
		String bookInfoPath = conf.get("book.info.path");
		String bookDeepReadFrequencyPath = conf.get("book.deep.read.frequency.path");
		String bookSeriesPath = conf.get("book.series.path");
		//add
		String freeBookPath = conf.get("free.book.path");
		
		URI [] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {			
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if(localFiles[i].toString().contains(bookInfoPath)){
					while( (line = in.readLine()) != null){			
						String fields[] = line.split("\\|",-1);
						if(fields.length == 2){
							bookClassMap.put(fields[0], fields[1]);					
						}				
					}
				}else if (localFiles[i].toString().contains(bookDeepReadFrequencyPath)) {
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						if(fields.length == 2) {					
							bookDeepReadFrequencyMap.put(fields[0], Integer.parseInt(fields[1]));						
						}					
					}					
				} else if (localFiles[i].toString().contains(bookSeriesPath)) {
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						String bookId = fields[0];
						String seriesId = fields[1];
						if(fields.length == 2) {//bookid|series_id							
							bookSeriesMap.put(bookId, seriesId);
							if (seriesBookSetMap.containsKey(seriesId)) {
								Set<String> bookSet = seriesBookSetMap.get(seriesId);
								bookSet.add(bookId);
							} else {
								Set<String> bookSet = new HashSet<String>();
								bookSet.add(bookId);
								seriesBookSetMap.put(seriesId, bookSet);
								
							}
									
						}					
					}					
				} else if (localFiles[i].toString().contains(freeBookPath)) {
					while( (line = in.readLine()) != null) {
						freeBookSet.add(line.trim());	
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
