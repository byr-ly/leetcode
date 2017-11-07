package com.eb.bi.rs.mras.seachrec.keyseach.offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;



public class KeyWordToTagDictMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Logger log = Logger.getLogger(KeyWordToTagDictMapper.class);	

	private HashSet<String> keyWords = new HashSet<String>();
	
	private int[] threeWeight = new int[7];
	private int[] fourWeight =  new int[7];
	private int[] generalWeight = new int[7];

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {		
		
		
		super.setup(context);
		
		Configuration conf = context.getConfiguration(); 		
		
		String[] weights = conf.get("three.tag.weight").split(",");
		if( weights.length == 7){
			for(int i = 0; i < weights.length; i++){
				threeWeight[i] = Integer.parseInt(weights[i]);
			}
		}else {
			log.error("three level weight length error");
		}
		weights = conf.get("four.tag.weight").split(",");
		if( weights.length == 7){
			for(int i = 0; i < weights.length; i++){
				fourWeight[i] = Integer.parseInt(weights[i]);
			}
		}else {
			log.error("three level weight length error");
		}
		weights = conf.get("general.tag.weight").split(",");
		if( weights.length == 7){
			for(int i = 0; i < weights.length; i++){
				generalWeight[i] = Integer.parseInt(weights[i]);
			}
		}else {
			log.error("three level weight length error");
		}


		String weightVectorString="";
		for(int i=0; i<7; i++){
			weightVectorString += threeWeight[i] + ",";
		}
		log.info("three level weight vector: " + weightVectorString);
		
		weightVectorString="";		
		for(int i=0; i<7; i++){
			weightVectorString += fourWeight[i] + ",";
		}
		log.info("four level weight vector: " + weightVectorString);
		
		
		weightVectorString="";
		for(int i=0; i<7; i++){
			weightVectorString += generalWeight[i] + ",";
		}
		log.info("general level weight vector: " + weightVectorString);
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {					
					String fields[] = line.split("\\|");
					keyWords.add(fields[0]);
				}

			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
	
	@Override	
	protected void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {

		String[] tags = value.toString().split("\\|",-1);
		tags[1] = tags[1] + ";" + tags[2];
		for(int i = 2; i < tags.length - 1; i++){
			tags[i] = tags[i+1];
		}
		
		for(String keyWord : keyWords){
			for(int idx = 0; idx < tags.length - 1; idx++){
				if(idx != 4){
					if(tags[idx].contains(keyWord)){						
						if(!"".equals(tags[2])){
							context.write(new Text(keyWord), new Text(tags[2]+ "|" + threeWeight[idx]));
						}
						if(!"".equals(tags[3])){
							context.write(new Text(keyWord), new Text(tags[3]+ "|" + fourWeight[idx]));							
						}
						
						String[] generalTags = tags[4].split(";");
						for(String generalTag : generalTags){
							if(!"".equals(generalTag))
							context.write(new Text(keyWord),new Text(generalTag + "|" + generalWeight[idx]));
						}						
					}				
				}else {
					if(tags[idx].contains(keyWord)){
						if(!"".equals(tags[2])){
							context.write(new Text(keyWord), new Text(tags[2]+ "|" + threeWeight[idx]));
						}
						if(!"".equals(tags[3])){
							context.write(new Text(keyWord), new Text(tags[3]+ "|" + fourWeight[idx]));							
						}						

						int index  = tags[idx].indexOf(keyWord);
						int bound1 = tags[idx].lastIndexOf(';', index);
						int bound2 = tags[idx].indexOf(';', index);	

						if(bound2 == -1){					
							bound2 = tags[idx].length();
						}
						
						String matchedtag = tags[idx].substring(bound1 + 1, bound2);						
						context.write(new Text(keyWord),new Text(matchedtag + "|" + generalWeight[idx]));						
						
					}

				}
			}
		}
	}
					

	
	
}

