package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class EntropyMapper extends Mapper<LongWritable, Text, Text, Text> {	


	private HashMap<String,double[]> sumIndicatorsMap = new HashMap<String,double[]>();

	private HashMap<String,double[]> indicatorEntropyMap = new HashMap<String, double[]>();	
	
	private HashMap<String,Long> rcdCountMap = new HashMap<String,Long>();
	
	private int indicatorCount;
	

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		super.setup(context);
	  
		Configuration conf = context.getConfiguration();	  
		indicatorCount = Integer.parseInt(conf.get("indicatorCount"));
 
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		/* data format:
		 * 0       sumIndicators:27.91121076233164:1930.0:19.57750757252926:16.036420022331104
		   0       rcdCount:1931
		 */	 
		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				System.out.println("file in cache:" + localFiles[i].toString());
				while ((line = in.readLine()) != null) {
					int index = line.indexOf('\t');
					String serviceType = line.substring(0, index).trim();
					String value = line.substring(index + 1).trim();
					String[] fields = value.split(":");
					if(fields[0].equals("sumIndicators")){
						double[] sumIndicators = new double[indicatorCount];
						for(int idx = 0; idx < indicatorCount; ++idx){							
							sumIndicators[idx] = Double.parseDouble(fields[idx + 1]);
						}						
						sumIndicatorsMap.put(serviceType, sumIndicators);						
					}
					else if(fields[0].equals("rcdCount")){
						rcdCountMap.put(serviceType, Long.parseLong(fields[1]));						
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
		String serviceType = fields[2];

		double[] sumIndicators = sumIndicatorsMap.get(serviceType);
		long rcdCount = rcdCountMap.get(serviceType);
		
		if(indicatorEntropyMap.containsKey(serviceType)){
			for(int idx = 0; idx < indicatorCount; ++idx){
				double disper = (Double.parseDouble(fields[idx + 3]) / sumIndicators[idx]); 				
				indicatorEntropyMap.get(serviceType)[idx] += (disper + 1) * Math.log(disper + 1) / Math.log(rcdCount);
			}			
		}
		else {
			double[] indicatorEntropy = new double[indicatorCount];
			for(int idx = 0; idx < indicatorCount; ++idx){
				double disper = (Double.parseDouble(fields[idx + 3]) / sumIndicators[idx]); 				
				indicatorEntropy[idx] = (disper + 1) * Math.log(disper + 1) / Math.log(rcdCount);
			}
			indicatorEntropyMap.put(serviceType,indicatorEntropy);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,	InterruptedException {		
		String writeValue = "";	
		Iterator<Entry<String, double[]>> iter = indicatorEntropyMap.entrySet().iterator();		
		while(iter.hasNext()){
			Entry<String, double[]> entry = iter.next();
			double[] indicatorEntroy = entry.getValue();
			for(int idx = 0; idx < indicatorCount; ++idx){
				writeValue += String.valueOf(indicatorEntroy[idx])+":";
			}
			context.write(new Text(entry.getKey()), new Text(writeValue.substring(0, writeValue.length()-1)));
			System.out.println(entry.getKey());
			System.out.println(writeValue.substring(0, writeValue.length()-1));
		}
		
		super.cleanup(context);		
	}
	
	
}

