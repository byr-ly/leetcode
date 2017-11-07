package com.eb.bi.rs.mras.bookrec.qiangfarec.fillersort;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FillerZoneReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	private String fieldDelimiter;
	private String fieldDelimiter2;
	private int cacheInfoLength;
	private Map<String, Map<String, Double>> sortOfZonePoints = new HashMap<String, Map<String, Double>>();
	
	@SuppressWarnings("resource")
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
		fieldDelimiter2 = conf.get("field.delimiter.2", "—");
		cacheInfoLength = Integer.parseInt(conf.get("cache.info.length", "3"));

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFiles == null) {
            System.out.println("local cacheFiles File is null");
            return;
        }
		for (int i = 0; i < cacheFiles.length; i++) {
            String line;
            BufferedReader br = new BufferedReader(new FileReader(cacheFiles[i].toString()));
            while ((line = br.readLine()) != null) {
            	String[] cacheStrs = line.split(fieldDelimiter);
				if (cacheStrs.length < cacheInfoLength) {
					continue;
				}
				String area = cacheStrs[0];
				String bookid = cacheStrs[1];
				double editScore = Double.parseDouble(cacheStrs[2]);
				Map<String, Double> bookPoints;
				if(sortOfZonePoints.containsKey(area)){
					bookPoints = sortOfZonePoints.get(area);
					bookPoints.put(bookid, editScore);
				} else {
					bookPoints = new HashMap<String, Double>();
					bookPoints.put(bookid, editScore);
				}
				
				sortOfZonePoints.put(area, bookPoints);
            }
        }
	}
	
	// key: 序号_zoneId  
	// value：bid|bid
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		Map<String, String> topNbookIDs = new HashMap<String, String>();
		for (Text pair : values) {
			String[] fields = pair.toString().split(fieldDelimiter);
			
			for ( int i = 0; i < fields.length; i++){// bookid
				topNbookIDs.put(fields[i], fields[i]);
			}
		}
		
		Iterator<Map.Entry<String, Map<String, Double>>> it = sortOfZonePoints.entrySet().iterator();
		while (it.hasNext()) {
			String output = "";
			
			Map.Entry<String, Map<String, Double>> en = it.next();
			
			String[] fields1 = key.toString().split(fieldDelimiter2);
			
			if( !fields1[1].equals(en.getKey()) ){
				continue;
			}
			
			Map<String, Double> zoneBookIDs = en.getValue();
			
			List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(zoneBookIDs.entrySet());

			Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
				// 降序排序
				public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
					// return o1.getValue().compareTo(o2.getValue());
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			
			String topNBookid = "";
			for (int i = 0; i < list.size(); i++) {  
                Entry<String,Double> ent=list.get(i);  
                
                String bookid = ent.getKey();//bookid
				
                if(topNbookIDs.containsKey(bookid)){// 查找top N 数据
					//System.out.println("topNbookIDs has bookid = " + bookid);
                	topNBookid += bookid + "|";
					continue;
				}
                
				output += bookid + "|";
            }

			output = key.toString() + ";" + topNBookid + output;
			context.write(NullWritable.get(), new Text(output));
		}
	}
}
