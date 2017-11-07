package com.eb.bi.rs.anhui.moduledev.itemcf.similarity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 两两组合的品牌的余弦相似度
 */
public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String,Double> weightMap = null ;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] fields = key.toString().split("\\|", -1);
		if(fields.length!=2) return;
		String i = fields[0];
		String j = fields[1];
		Double wi = weightMap.get(i);
		Double wj = weightMap.get(j);
		Double sum = 0.0;
		for(Text value : values){
			String[] score = value.toString().split("\\|", -1);
			if(score.length==2){
				sum += Double.parseDouble(score[0])*Double.parseDouble(score[1]);
			}
		}

		context.write(new Text(i+"|"+j+"|"+(sum/(wi*wj))), new Text());
		context.write(new Text(j+"|"+i+"|"+(sum/(wi*wj))), new Text());
		
	}
	
	//读取单个品牌的评分平方和的平方根，即用作余弦相似计算的分母部分
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		weightMap = new HashMap<String,Double>();
		Configuration conf = context.getConfiguration();
		
		System.out.printf("reduce setup");
        
		URI[] localFiles = context.getCacheFiles();
		if(localFiles==null){
			System.out.println("单个拆分的品牌评分读取失败，即余弦相似度分母读取失败");
			return;
		}
		for (URI path : localFiles) {
			BufferedReader br = null;
			String line = null;
			try {
				FileSystem fs = FileSystem.get(path, conf);
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
				String[] fields = null;
				while ((line = br.readLine()) != null) {
					fields = line.split("\\|");
					if (fields.length != 2) {
						continue;
					}
					weightMap.put(fields[0], Double.parseDouble(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}
	
}

