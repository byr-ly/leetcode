package com.eb.bi.rs.qhll.userapprec.userapppref;

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
//import org.apache.log4j.Logger;



class UserAppPrefMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final Text writeKey = new Text("");
	private HashMap<String,double[]> maxIndicatorsMap = new HashMap<String,double[]>();
	private HashMap<String,double[]> indicatorWeightMap = new HashMap<String, double[]>();
	private int indicatorCount;

	
	@Override	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {		
		
		
		String fields[] = value.toString().split("\\|");		
			
		String serviceType = fields[2];
		double[] indicatorWeight = indicatorWeightMap.get(serviceType);
		double[] maxIndicator = maxIndicatorsMap.get(serviceType);
		double sum = 0.0;
		for(int idx = 0; idx < indicatorCount; ++idx){
			sum += Double.parseDouble(fields[idx + 3]) * indicatorWeight[idx] / maxIndicator[idx];
		}
		String writeValue = fields[0] + "|" + fields[1] + "|" + String.valueOf(sum);
		context.write(writeKey, new Text(writeValue));

	}
		

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		
		super.setup(context);
	  
		Configuration conf = context.getConfiguration();	  
		indicatorCount = Integer.parseInt(conf.get("indicatorCount"));
 
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		/* data format:
		 *
		   0       maxIndicators:1.0:1.0:1.0:1.0
			0       minIndicators:0.0:1.0:0.0:0.0
			0       sumIndicators:27.91210762331819:1931.0:19.57760354881928:16.03873171011727
			0       rcdCount:1931
		   
		 */
		/*0       0.1324986721088442:0.13220806479504482:0.13298488602776443:0.13263696419899132*/
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
					if(fields[0].equals("maxIndicators")){
						double[] maxIndicators = new double[indicatorCount];
						for(int idx = 0; idx < indicatorCount; ++idx){							
							maxIndicators[idx] = Double.parseDouble(fields[idx + 1]);
						}						
						maxIndicatorsMap.put(serviceType, maxIndicators);					
					}
//					else if(fields[0].equals("minIndicators")){
//						double[] minIndicators = new double[indicatorCount];
//						for(int idx = 0; idx < indicatorCount; ++idx){							
//							minIndicators[idx] = Double.parseDouble(fields[idx + 1]);
//						}						
//						minIndicatorsMap.put(serviceType, minIndicators);					
//					}
					else if(fields[0].indexOf("Indicators") == -1 && !fields[0].equals("rcdCount")){
						double[] indicatorWeight = new double[indicatorCount];
						double sum = 0.0;
						for(int idx = 0; idx < indicatorCount; ++idx){							
							sum += 1 - Double.parseDouble(fields[idx]);
						}	
						for(int idx = 0; idx < indicatorCount; ++idx){							
							indicatorWeight[idx] = (1 - Double.parseDouble(fields[idx])) / sum;
							System.out.println("weight:" + indicatorWeight[idx]);
						}	
						indicatorWeightMap.put(serviceType, indicatorWeight);					
					}
				}
	
			} finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
}

