package com.eb.bi.rs.opus.correlation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CorrelationRecResultMapper extends Mapper<Object, Text, Text, Text> {
	private float cosThresholdValue;
	private float irThresholdValue;
	private Set<String> rec_ranks = new HashSet<String>();
	private Set<String> rec_sours = new HashSet<String>();
	private Map<String , String> opus_rank = new HashMap<String, String>();
	private Map<String , String> opus_sour = new HashMap<String, String>();
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (fields.length == 2) {
			String[] correlationInfos = fields[1].split("\\|");
			if (correlationInfos.length != 8) {
				return;
			}
			if (!(rec_ranks.contains(opus_rank.get(correlationInfos[0])) || rec_sours.contains(opus_sour.get(correlationInfos[0])))) {
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
		String[] ranks = configuration.get("rec.ranks").split(",");
		for (String rank : ranks) {
			rec_ranks.add(rank);
		}
		String[] sours = configuration.get("rec.sours").split(",");
		for (String sour : sours) {
			rec_sours.add(sour);
		}
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
					String[] fields = line.split("\\|");//格式：opus_id动漫ID|opus_type内容类型（动画或漫画）|subject_id主题|content_sour内容类型（普通0、原创1）|作品评级|cp_id|opus_name动漫名称
					if (fields.length >= 6) {
						opus_sour.put(fields[0], fields[3]);
						opus_rank.put(fields[0], fields[4]);
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
