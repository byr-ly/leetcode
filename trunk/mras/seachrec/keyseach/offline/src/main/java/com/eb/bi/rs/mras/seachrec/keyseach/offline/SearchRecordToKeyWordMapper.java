package com.eb.bi.rs.mras.seachrec.keyseach.offline;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;


public class SearchRecordToKeyWordMapper extends Mapper<Text, Text, Text, IntWritable> {	
	
	private HashSet<String> stopWords = new HashSet<String>();
	private HashMap<String,Integer>	keyWords = new HashMap<String,Integer>();

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		super.setup(context);	  
		Configuration conf = context.getConfiguration(); 
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);	

		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				String filePath = localFiles[i].toString();
				in = new BufferedReader(new FileReader(filePath));
				while ((line = in.readLine()) != null) {
					stopWords.add(line);					
				}
			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}				
	}
	
	@Override	
	protected void map(Text key, Text value, Context context) 
		throws IOException, InterruptedException {

		//String sentence = value.toString().substring(1);
		String sentence = value.toString();	
		JiebaSegmenter segmenter = new JiebaSegmenter();
		
		List<SegToken> tokens = segmenter.process(sentence, SegMode.SEARCH);
		for(SegToken token: tokens){
			String keyword = token.token;
			if(!stopWords.contains(keyword)){
				if(keyWords.containsKey(keyword)){					
					keyWords.put(keyword, keyWords.get(keyword) + 1);
				}
				else{
					keyWords.put(keyword, 1);
				}
			}
		}			 
	 }
	
	protected void cleanup(Context context) throws IOException ,InterruptedException
	{
		Iterator<Entry<String, Integer>> iter = keyWords.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, Integer> entry = iter.next();
			context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		}
	}
}

