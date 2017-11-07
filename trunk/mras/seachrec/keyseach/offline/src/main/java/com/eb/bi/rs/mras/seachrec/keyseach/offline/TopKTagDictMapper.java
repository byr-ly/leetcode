package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TopKTagDictMapper extends Mapper<LongWritable, Text, Text, TagDictWritable>{

	HashMap<String, TreeSet<TagDictWritable>> topKMap = new HashMap<String, TreeSet<TagDictWritable>>();
	HashSet<String> specialTagSet = new HashSet<String>();	
	
	int k = 5; 
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String[] fields = value.toString().split("\\|");		

		String tag = fields[1];	
		String keyWord = fields[0];
		
		if(specialTagSet.contains(tag)){
			boolean specialTagContainsKeyWord = false;
			for(String specialTag : specialTagSet){
				if(specialTag.contains(keyWord)){
					specialTagContainsKeyWord = true;
				}
			}
			if(!specialTagContainsKeyWord){
				return;
			}
		}
		//if(!specialTagSet.contains(tag) || specialTagContainsKeyWord){			
		int weight = Integer.parseInt(fields[2]);
		TagDictWritable tagDict = new TagDictWritable(tag,weight);			
		if(topKMap.containsKey(keyWord)){
			TreeSet<TagDictWritable> set = topKMap.get(keyWord);
			set.add(tagDict);
			if(set.size() > k) {
				set.remove(set.first());				
			}		
		}
		else{
			TreeSet<TagDictWritable> set = new TreeSet<TagDictWritable>();
			set.add(tagDict);
			topKMap.put(keyWord, set);			
		}				
		//}	
	}
	
	protected void setup(Context context) throws IOException ,InterruptedException {

		super.setup(context);
		  
		Configuration conf = context.getConfiguration(); 	
		k = conf.getInt("topk.number", 5);
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {					
					String fields[] = line.split("\\|");
					specialTagSet.add(fields[0]);
				}

			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
	

	protected void cleanup(Context context) throws IOException ,InterruptedException {		
		Iterator<Entry<String, TreeSet<TagDictWritable>>> iter= topKMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, TreeSet<TagDictWritable>> entry = iter.next();
			Text keyWord = new Text(entry.getKey());
			Iterator<TagDictWritable> setIter = entry.getValue().iterator();
			while(setIter.hasNext()){
				context.write(keyWord, setIter.next());
			}
		}		
	}	
}

