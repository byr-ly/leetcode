package com.eb.bi.rs.mras.seachrec.keyseach.offline;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;



import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NormalizeTagDictMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	HashMap<String, Integer> sumMap = new HashMap<String,Integer>();
	
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String[] fields = value.toString().split("\\|");		
		String keyWord = fields[0];	
		double NormlizedWeight = Double.parseDouble(fields[2])/sumMap.get(keyWord);
		
		Text outkeyText = new Text(keyWord + "|" + fields[1] + "|" + NormlizedWeight);
		context.write(outkeyText, NullWritable.get());				
	}
	
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());		

		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {					
					String fields[] = line.split("\\|");
					sumMap.put(fields[0], Integer.parseInt(fields[1]));

				}

			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}

	}

}

