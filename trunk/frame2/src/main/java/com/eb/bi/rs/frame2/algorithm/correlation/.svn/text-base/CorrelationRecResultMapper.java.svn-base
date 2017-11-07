package com.eb.bi.rs.frame2.algorithm.correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CorrelationRecResultMapper extends Mapper<Object, Text, Text, Text> {
	private float cosThresholdValue;
	private float irThresholdValue;
	private Set<String> rec_ids = new HashSet<String>();
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (fields.length == 2) {
			String[] correlationInfos = fields[1].split("\\|");
			if (correlationInfos.length != 8) {
				return;
			}
			if (!(rec_ids.contains(correlationInfos[0]))) {
				return;
			}
			if (Float.parseFloat(correlationInfos[6]) < cosThresholdValue) {
				return;
			}
			if (Float.parseFloat(correlationInfos[7]) > irThresholdValue) {
				return;
			}
	
			context.write(new Text(fields[0]), new Text(correlationInfos[0] + "|" + correlationInfos[5]));
		}
	}

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		cosThresholdValue = Float.parseFloat(configuration.get("cos.threshold.value"));
		irThresholdValue = Float.parseFloat(configuration.get("ir.threshold.value"));
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(localFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				while((line = in.readLine()) != null) { 
					String[] fields = line.split("\\|");//格式：物品ID|。。。
					rec_ids.add(fields[0]);			
				}				
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}
	}
	
}
