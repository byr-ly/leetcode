package com.eb.bi.rs.mras.seachrec.keyseach.offline;



import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyWordToTagDictReducer extends Reducer<Text,Text, Text, Text> {

	@Override
    public void reduce(Text keyWord, Iterable<Text> tagWeights, Context context)
            throws IOException, InterruptedException {		
		
		HashMap<String,Integer> tagWeightMap = new HashMap<String, Integer>();
        for ( Text tagWeight : tagWeights) {

        	String[] fields	= tagWeight.toString().split("\\|");
        	String tag	= fields[0];
        	int weight = Integer.parseInt(fields[1]);
        	if(tagWeightMap.containsKey(tag)){
        		tagWeightMap.put(tag, tagWeightMap.get(tag) + weight);
        	}else {
        		tagWeightMap.put(tag, weight);
			}         	
        }
        Iterator<Entry<String, Integer>> iterator = tagWeightMap.entrySet().iterator();
        while(iterator.hasNext()){
        	Entry<String, Integer> entry = iterator.next();
        	context.write(keyWord, new Text(entry.getKey()+ "|" + entry.getValue()));        	
        }
  }
}
