package com.eb.bi.rs.mras.seachrec.relatedkey.offline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FilterHotwordMapper extends Mapper<Text, Text, Text, Text>{
	private Map<String,Integer> hotwordList = new TreeMap<String,Integer>();
	private Set<String> haveresultwordList = new HashSet<String>();
	private String m_1_separator;
	private int topN;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		m_1_separator = context.getConfiguration().get("id_num_separator");
		topN = Integer.valueOf(context.getConfiguration().get("hot_top_num")).intValue();
		
		super.setup(context);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());		

		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				//有结果关键词加载
				if(localFiles[i].toString().contains("have_result")) {
					while ((line = in.readLine()) != null) {					
						String fields[] = line.split("\\|");
						haveresultwordList.add(fields[0]);
					}
				}
				//热词表加载
				if(localFiles[i].toString().contains("frequency")) {
					while ((line = in.readLine()) != null) {					
						String fields[] = line.split("\\|");
						hotwordList.put(fields[0],Integer.valueOf(fields[1]));
						
						if(hotwordList.size() > topN){
							List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>( 
									hotwordList.entrySet());
							//排序
							Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>(){
								public int compare(Map.Entry<String, Integer> o1,
								Map.Entry<String, Integer> o2){
								return ((int)(o2.getValue() - o1.getValue()));
								}
								});
							hotwordList.remove(infoIds.get(infoIds.size()-1).getKey());
						}
					}
				}
			} 
			finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] values = value.toString().split(m_1_separator);
		
		if(hotwordList.containsKey(values[0])){
			return;
		}
		
		if(haveresultwordList.contains(values[0])){
			
		}
		else{
			return;
		}
		
		context.write(key,value);
	}
}
