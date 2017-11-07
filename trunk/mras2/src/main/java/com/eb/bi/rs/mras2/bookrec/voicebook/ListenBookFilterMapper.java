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
import java.util.Map;

public class ListenBookFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	//听书用户深度收听阈值，默认是50
	private int deepListenThreshold;
	
	//智能演播-听书分类ID
	private String intellBroadcast = "878";
	
	//bookBigClassMap中存放的数据是：<bookId, bigClassId>
	private Map<String, String> bookBigClassMap = new HashMap<String, String>();
	//bookClassMap中存放的数据是：<bookId, ClassId>
	private Map<String, String> bookClassMap = new HashMap<String, String>();
	
	//bookDeepReadFrequencyMap中存放的数据是：<bookId, listen_cnt>
	private Map<String, Integer> bookDeepListenFrequencyMap = new HashMap<String, Integer>();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype 
		String[] fields = value.toString().split("\\|",-1);
		assert fields.length == 10;
		if(fields.length == 10){			
			//A、B图书同属于一个大类或者A书属于“智能演播-听书”分类
			//B书的深度收听用户数大于一定阈值，默认是50
			//AB共同的收听用户数大于一定阈值，默认是5(这一步的过滤是在协同过滤矩阵那里进行的，将min_num变量设置为5就可以过滤掉共同用户数低于5的条目)
			String book1 = fields[0];
			String book2 = fields[1];
			if(bookBigClassMap.containsKey(book1) && bookBigClassMap.containsKey(book2) && bookDeepListenFrequencyMap.containsKey(book2)) {
				String book1BigClass = bookBigClassMap.get(book1);
				String book2BigClass = bookBigClassMap.get(book2);
				String book1Class = bookClassMap.get(book1);
				if((!"".equals(book1BigClass) && book1BigClass.equals(book2BigClass) && bookDeepListenFrequencyMap.get(book2) > deepListenThreshold ) || (!"".equals(book1Class) && book1Class.equals(intellBroadcast))) {
					context.write(value, NullWritable.get());
				}
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException 	{
		
		Configuration conf = context.getConfiguration();
		deepListenThreshold = conf.getInt("deep.listen.threshold", 50);
		String bookInfoPath = conf.get("book.info.path");
		String bookDeepListenFrequencyPath = conf.get("book.deep.listen.frequency.path");
		
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {			
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if(localFiles[i].toString().contains(bookInfoPath)){
					//听书表，数据格式为：bookId|bigClassId|classId
					while( (line = in.readLine()) != null){			
						String fields[] = line.split("\\|",-1);
						if(fields.length == 3){
							bookBigClassMap.put(fields[0], fields[1]);	
							bookClassMap.put(fields[0], fields[2]);
						}				
					}
				}else if (localFiles[i].toString().contains(bookDeepListenFrequencyPath)) {
					//听书6个月表现表，数据格式为：bookId|listen_cnt(6个月收听)
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						if(fields.length == 2) {					
							bookDeepListenFrequencyMap.put(fields[0], Integer.parseInt(fields[1]));						
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
