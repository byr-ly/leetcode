package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final Text writeKey = new Text("");
	private double[] maxIndicators;
	private double[] minIndicators;
	private int indicatorCount;
	

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		super.setup(context);
	  
		Configuration conf = context.getConfiguration();	  
		indicatorCount = Integer.parseInt(conf.get("indicatorCount"));		
		maxIndicators = new double[indicatorCount];
		minIndicators = new double[indicatorCount];
	  
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) {				
					String[] fields = line.split(":");
					for(int idx = 0; idx < indicatorCount; ++idx){
						maxIndicators[idx] = Double.parseDouble(fields[2 * idx]);
						minIndicators[idx] = Double.parseDouble(fields[2 * idx + 1]);
					}
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

		
		
		String[] fields = value.toString().split("\\|");
		if(fields.length < 3 + indicatorCount){
			return;
		}
		UserAppInfo userAppInfo = new UserAppInfo();
		userAppInfo.setMSISDN(fields[0]);
		userAppInfo.setServiceCode(fields[1]);
		userAppInfo.setServiceType(fields[2]);
		userAppInfo.setIndicatorCnt(indicatorCount);
		
		
		double[] normIndicators = new double[indicatorCount];
		for(int idx = 0; idx < indicatorCount; ++idx){
			double tmp = Double.parseDouble(fields[idx + 3]);
			if(maxIndicators[idx] == minIndicators[idx]){
				normIndicators[idx] = 1;
			}
			else {
				normIndicators[idx] = ( tmp - minIndicators[idx]) / (maxIndicators[idx]-minIndicators[idx]);
			}				
		}
		userAppInfo.setIndicators(normIndicators);
		context.write(writeKey, new Text(userAppInfo.toString()));						
	}
}

