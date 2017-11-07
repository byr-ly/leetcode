package com.eb.bi.rs.qhll.userapprec.userapppref;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

class ServiceTypeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private HashMap<String,double[]> maxIndicatorsMap = new HashMap<String,double[]>();
	private HashMap<String,double[]> minIndicatorsMap = new HashMap<String,double[]>();	
	private HashMap<String,double[]> sumIndicatorsMap = new HashMap<String,double[]>();
	private HashMap<String,Long> rcdCountMap = new HashMap<String,Long>();
	private int indicatorCount;

	
	@Override	
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {		
		
		
		String fields[] = value.toString().split("\\|");
		
		String serviceType = fields[2];
		
		if(maxIndicatorsMap.containsKey(serviceType)){/*只要其中一个map包含改业务类型，其他的map必然包含该业务类型*/
			for(int idx = 0; idx < indicatorCount; ++idx){
				double tmp = Double.parseDouble(fields[idx + 3]);
				
				if( tmp > maxIndicatorsMap.get(serviceType)[idx]){
					maxIndicatorsMap.get(serviceType)[idx] = tmp;
				}
				if( tmp < minIndicatorsMap.get(serviceType)[idx]){
					minIndicatorsMap.get(serviceType)[idx] = tmp;
				}
				sumIndicatorsMap.get(serviceType)[idx] += tmp;
			}
			rcdCountMap.put(serviceType, rcdCountMap.get(serviceType) + 1L);
		}
		else {
			double[] sumIndicators = new double[indicatorCount];
			double[] maxIndicators = new double[indicatorCount];
			double[] minIndicators = new double[indicatorCount];			
		
			for(int idx = 0; idx < indicatorCount; ++idx){
				maxIndicators[idx] = Double.parseDouble(fields[idx + 3]);
				minIndicators[idx] = Double.parseDouble(fields[idx + 3]);
				sumIndicators[idx] = Double.parseDouble(fields[idx + 3]);
			}
			maxIndicatorsMap.put(serviceType, maxIndicators);
			minIndicatorsMap.put(serviceType, minIndicators);
			sumIndicatorsMap.put(serviceType, sumIndicators);
			rcdCountMap.put(serviceType, 1L);
						
		}

	}
		

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	  super.setup(context);

	  indicatorCount = Integer.parseInt(context.getConfiguration().get("indicatorCount")); 
	 
	}
	@Override
	 /**
	   * Create a new Canopy containing the given point and canopyId
	   * 
	   * @param center a point in vector space
	   * @param canopyId an int identifying the canopy local to this process only
	   * @param measure a DistanceMeasure to use
	   */
	protected void cleanup(Context context) throws IOException,	InterruptedException {		
		String writeValue;		
		Iterator<Entry<String, double[]>> iter;	
		/*write the max values of indicators group by service type*/
		writeValue = "maxIndicators:";
		iter = maxIndicatorsMap.entrySet().iterator();		
		while(iter.hasNext()){
			Entry<String, double[]> entry = iter.next();
			double[] maxIndicators = entry.getValue();
			for(int idx = 0; idx < indicatorCount; ++idx){
				writeValue += String.valueOf(maxIndicators[idx])+":";
			}
			context.write(new Text(entry.getKey()), new Text(writeValue.substring(0, writeValue.length()-1)));
			System.out.println(entry.getKey());
			System.out.println(writeValue.substring(0, writeValue.length()-1));
		}
		/*write the min values of indicators group by service type */
		iter = minIndicatorsMap.entrySet().iterator();
		writeValue = "minIndicators:";
		while(iter.hasNext()){
			Entry<String, double[]> entry = iter.next();
			double[] minIndicators = entry.getValue();
			for(int idx = 0; idx < indicatorCount; ++idx){
				writeValue += String.valueOf(minIndicators[idx])+":";
			}
			context.write(new Text(entry.getKey()), new Text(writeValue.substring(0, writeValue.length()-1)));
			System.out.println(entry.getKey());
			System.out.println(writeValue.substring(0, writeValue.length()-1));
		}
		/*write the min values of indicators group by service type */
		iter = sumIndicatorsMap.entrySet().iterator();
		writeValue = "sumIndicators:";
		while(iter.hasNext()){
			Entry<String, double[]> entry = iter.next();
			double[] sumIndicators = entry.getValue();
			for(int idx = 0; idx < indicatorCount; ++idx){
				writeValue += String.valueOf(sumIndicators[idx])+":";
			}
			context.write(new Text(entry.getKey()), new Text(writeValue.substring(0, writeValue.length()-1)));
			System.out.println(entry.getKey());
			System.out.println(writeValue.substring(0, writeValue.length()-1));
		}
		
		Iterator<Entry<String, Long>> iter1 = rcdCountMap.entrySet().iterator();
		writeValue = "rcdCount:";
		while(iter1.hasNext()){			
			Entry<String, Long> entry = iter1.next();			
			context.write(new Text(entry.getKey()), new Text(writeValue + String.valueOf(entry.getValue())));
			System.out.println(entry.getKey());
			System.out.println(String.valueOf(entry.getValue()));
		}


		super.cleanup(context);		
	}
}
