package com.eb.bi.rs.mras.bookrec.guessyoulike;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.frame.recframe.resultcal.offline.selector.util.StringDoublePair;

/*
 * 各个版面选取TOP 80，不足80的随机补白，此处涉及补白库即BI推荐库中满足各自图书限制的图书均可补白。
 */

public class SelectTop4UnifiedInterfaceReducer extends Reducer<Text, StringDoublePair, NullWritable , Text> {
	
	
	
	Map<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
	private int selectNumber;
	
	
	private Random random = new Random();
	
	@Override
	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws IOException ,InterruptedException {
		
		TreeSet<StringDoublePair> set = new TreeSet<StringDoublePair>();
		for (StringDoublePair value : values) {
			StringDoublePair pair = new StringDoublePair(value);
			if (set.size() < selectNumber) {
				set.add(pair);					
			} else if (pair.compareTo(set.first()) > 0) {
				set.remove(set.first());
				set.add(pair);
			}					
		}
		
		HashSet<String> books = new HashSet<String>();
		for (StringDoublePair pair : set) {
			books.add(pair.getFirst());
		}
		
		String[] fields = key.toString().split("\\|"); //用户|版面
		String user = fields[0];
		String page = fields[1];
		ArrayList<String> list = map.get(page);		
		
		while (books.size() < selectNumber) {//用推荐库中的图书补白					
			books.add(list.get(random.nextInt(list.size())));		
		}	
		
		for (String book : books) {
			context.write(NullWritable.get(), new Text(user + "|" + book));
		}
		
	}
	
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		
		
		
		Configuration conf = context.getConfiguration();	
		
		
		selectNumber = conf.getInt("select.number.per.page", 80);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);		
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((line = in.readLine()) != null) { //图书ID|定制标签|版面集						
					String[] fields = line.split("\\|", -1);
					if (fields.length == 3) {
						String book = fields[0];
						String[] pages = fields[2].split(",");
						for (String page : pages) {
							if (map.containsKey(page)) {
								ArrayList<String> list = map.get(page);
								list.add(book);
							} else {
								ArrayList<String> list = new ArrayList<String>();
								list.add(book);
								map.put(page, list);
							}
						}
						
					}
					
				}			
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}	
	}

}
