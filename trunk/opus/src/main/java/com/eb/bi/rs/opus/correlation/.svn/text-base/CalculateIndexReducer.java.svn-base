package com.eb.bi.rs.opus.correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CalculateIndexReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String,List<String>> appearTimesMap = new TreeMap<String,List<String>>();
	private float supportThresholdValue;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		float Anum = 0;
		float Afrequence = 0;
		float Bnum = 0;
		float ABsupport = 0;
		double ABcos = 0;
		float ir = 0;		
		float AB_A = 0;
		float AB_B = 0;
		
		Anum = Float.valueOf(appearTimesMap.get(key.toString()).get(0));
		Afrequence = Float.valueOf(appearTimesMap.get(key.toString()).get(2));
		for (Text value : values){
			String[] fields;
			fields = value.toString().split("\\|");
			if (fields.length < 2) {
				continue;
			}
			Bnum = Float.valueOf(appearTimesMap.get(fields[0]).get(0));
			
			AB_A = Float.valueOf(fields[1])/Anum;
			AB_B = Float.valueOf(fields[1])/Bnum;
			ABsupport = AB_A * Afrequence;
			if (ABsupport < supportThresholdValue) {
				continue;
			}
			ABcos = Float.valueOf(fields[1])/Math.sqrt(Bnum*Anum);
			ir = Bnum/Anum;
			DecimalFormat decimalFormat=new DecimalFormat(".000");
			//输出结果：动漫A 动漫B|动漫A用户数|动漫B用户数|动漫AB共同用户数|支持度|置信|余弦值|IR
			context.write(key, new Text(fields[0] + "|" + Anum + "|" +Bnum+ "|" +fields[1] 
					+"|" + decimalFormat.format(ABsupport) +"|" + decimalFormat.format(AB_A) +"|" + decimalFormat.format(ABcos) +"|" + decimalFormat.format(ir) +"|"));
			context.write(new Text(fields[0]), new Text(key + "|" + Bnum + "|" +Anum+ "|" +fields[1] 
					+"|" + decimalFormat.format(ABsupport) +"|" + decimalFormat.format(AB_B) +"|" + decimalFormat.format(ABcos) +"|" + decimalFormat.format(1/ir) +"|" ));
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration configuration = context.getConfiguration();
		supportThresholdValue = Float.parseFloat(configuration.get("support.threshold.value"));
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(localFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				while((line = in.readLine()) != null) { 
					String[] fields = line.split("\t");
					String[] items = fields[1].split("\\|");
					if (fields.length == 2 && items.length == 3) {
						List<String> list = new ArrayList<String>();
						for (int j = 0; j < items.length; j++) {
							list.add(items[j]);
						}
						appearTimesMap.put(fields[0], list);
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
