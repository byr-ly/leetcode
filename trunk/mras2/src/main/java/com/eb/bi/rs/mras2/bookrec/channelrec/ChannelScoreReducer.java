package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelScoreReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	private Double baseScore;
	private Double increaseScore;
	private Map<String, String> bookInfo = new HashMap<String, String>();

	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		String msisdn = key.toString();
		Set<String> channelTypes= new HashSet<String>();
		Map<String , Double> historyBookScore = new HashMap<String, Double>();
		Map<String , Double> recentTwoDaysBookScore = new HashMap<String, Double>();
		StringBuffer filterBooks = new StringBuffer();
		for (Text text : values) {
			String[] fields = text.toString().split("\\|");
			if ("0".equals(fields[0])) {
				channelTypes.add(fields[1]);
			} else if ("1".equals(fields[0])) {
				for (int i = 1; i < fields.length ; i++) {
					String[] items = fields[i].split(",");
					if (items.length == 2) {
						historyBookScore.put(items[0], Double.parseDouble(items[1]));						
					}
				}
			} else if ("2".equals(fields[0])) {
				for (int i = 1 ; i < fields.length ; i++) {
					recentTwoDaysBookScore.put(fields[i], baseScore);
				}
			} else if ("3".equals(fields[0])) {
				for (int i = 1 ; i < fields.length ; i++) {
					filterBooks.append(fields[1] + ",");
				}
			}
		}
		if(channelTypes.isEmpty()){
			return;
		}
		
		StringBuffer result = new StringBuffer();
		
		for (String book : historyBookScore.keySet()) {
			Double score = historyBookScore.get(book);
			
			if (recentTwoDaysBookScore.get(book) != null) {
				score += increaseScore;
				historyBookScore.put(book, score);
				recentTwoDaysBookScore.remove(book);
			}
			result.append(book + "," + bookInfo.get(book) + "," + score + "|");	
		}
		for (String book : recentTwoDaysBookScore.keySet()) {
			result.append(book + "," + bookInfo.get(book) + "," + recentTwoDaysBookScore.get(book) + "|");	
		}
		for (String channelType : channelTypes) {
			//输出格式：msisdn;channel_type;bookid,classid,book_score|...
			context.write(new Text(msisdn + ";" + channelType + ";" + result.toString() + ";" + filterBooks.toString()), NullWritable.get());
		}			
	
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		baseScore = Double.parseDouble(conf.get("baseScore"));
		increaseScore = Double.parseDouble(conf.get("increaseScore"));
		//Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(localFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				while((line = in.readLine()) != null) { //bookid图书ID|authorid作者ID|classid分类ID|charge_type计费类型|series_id系列号					
					String[] fields = line.split("\\|", -1);
					bookInfo.put(fields[0] , fields[2]);						
				}			
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}
	}
	
}
