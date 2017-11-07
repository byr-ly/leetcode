package com.eb.bi.rs.qhll.userapprec.predictpref;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemIdReplaceMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static HashMap<String, String> itemIdNameMap = new HashMap<String, String>();
	private final Text writeKey = new Text("");	

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();	  
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {
					String[] fields = line.split("\t");
					itemIdNameMap.put(fields[0], fields[1]);
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
		String userFields[] = value.toString().split("\t");
		if (userFields.length != 2){
			return;
		}
		
		String userId = userFields[0];
		String items = userFields[1];
		items = items.substring(1, items.length()-2);
		String itemFields[] = items.split(",");
		
		for (int i=0; i<itemFields.length; i++) {
			String itemPref[] = itemFields[i].split(":");
			String result = userId + "|" + itemIdNameMap.get(itemPref[0]) + "|" + itemPref[1];
			context.write(writeKey, new Text(result));
		}
	}
}

