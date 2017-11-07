package com.eb.bi.rs.mras.bookrec.guessyoulike;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras.bookrec.guessyoulike.util.TextPair;


public class Join4UserBookPrefFinalScoreReducer extends Reducer<TextPair, TextPair, NullWritable, Text> {

	private double[] weights;
	private int simCount;
	private int propCount;
	
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {		
		int count = 0;
		String userId = key.getFirst().toString();
		double[] maxSimilarities = null;
		double[] maxProperties = null;
		if (propCount == 0) {
			for (TextPair pair: values) {			
				if (pair.getSecond().toString().equals("0")) {
					String[] maxSimilarityStrs = pair.getFirst().toString().split(",");
					if(maxSimilarityStrs.length == simCount) {
						maxSimilarities = new double[simCount];
						for (int idx = 0; idx < maxSimilarityStrs.length; idx++) {
							maxSimilarities[idx] = Double.parseDouble(maxSimilarityStrs[idx]);
						}
					}else {
						String msg = String.format("user[%s]'s max similarities[%s]'s length is not equal to configuration similarity.count[%d]", userId, pair.getFirst().toString(), simCount);
						throw new RuntimeException(msg);
					}
					++count;
				}else {
					if(count != 1) {
						String msg = String.format("user[%s]'s max value record count is not exactly 1", userId);
						throw new RuntimeException(msg);					
					}
					//用户|待推荐图书|源图书集|相似度|属性偏好，eg。u1|a1|1:b1,b3|0.1,0.2,0.3|1.0,4.0,9.0				
					double score = 0.0;
					String[] fields = pair.getFirst().toString().split("\\|",-1);
					String[] similaties = fields[2].split(",");
					if (similaties.length == simCount) {
						for (int idx = 0; idx < similaties.length; idx++) {
							if (maxSimilarities[idx] != 0) {
								score += Double.parseDouble(similaties[idx]) / maxSimilarities[idx] * weights[idx];
							}
						}					
					}else {
						String msg = String.format("user[%s]'s rec record[%s]'s similarities length is not equal to configuration similarity.count[%d]", userId, pair.getFirst().toString(), simCount);
						throw new RuntimeException(msg);
					}			
					String result = userId + "|" + fields[0] + "|" + fields[1] + "|" + score;
					context.write(NullWritable.get(), new Text(result));				
				}	
			}
			
		} else {
			for (TextPair pair: values) {			
				if (pair.getSecond().toString().equals("0")) {

					String[] fields = pair.getFirst().toString().split("\\|");
					String[] maxSimilarityStrs = fields[0].split(",");
					if(maxSimilarityStrs.length == simCount) {
						maxSimilarities = new double[simCount];
						for (int idx = 0; idx < maxSimilarityStrs.length; idx++) {
							maxSimilarities[idx] = Double.parseDouble(maxSimilarityStrs[idx]);
						}
					}else {
						String msg = String.format("user[%s]'s max similarities[%s]'s length is not equal to configuration similarity.count[%d]", userId, fields[0], simCount);
						throw new RuntimeException(msg);
					}
					String[] maxPropertyStrs = fields[1].split(",");
					if (maxPropertyStrs.length == propCount) {
						maxProperties = new double[propCount];
						for (int idx = 0; idx < maxPropertyStrs.length; idx++) {
							maxProperties[idx] = Double.parseDouble(maxPropertyStrs[idx]);
						}					
					}else {
						String msg = String.format("user[%s]'s max properties[%s]'s length is not equal to configuration property.count[%d]", userId, fields[1], propCount);
						throw new RuntimeException(msg);
					}
					++count;
				}else {
					if(count != 1) {
						String msg = String.format("user[%s]'s max value record count is not exactly 1", userId);
						throw new RuntimeException(msg);					
					}
					//用户|待推荐图书|源图书集|相似度|属性偏好，eg。u1|a1|1:b1,b3|0.1,0.2,0.3|1.0,4.0,9.0				
					double score = 0.0;
					String[] fields = pair.getFirst().toString().split("\\|",-1);
					String[] similaties = fields[2].split(",");
					if (similaties.length == simCount) {
						for (int idx = 0; idx < similaties.length; idx++) {
							if (maxSimilarities[idx] != 0) {
								score += Double.parseDouble(similaties[idx]) / maxSimilarities[idx] * weights[idx];
							}
						}					
					}else {
						String msg = String.format("user[%s]'s rec record[%s]'s similarities length is not equal to configuration similarity.count[%d]", userId, pair.getFirst().toString(), simCount);
						throw new RuntimeException(msg);
					}
					String[] properties = fields[3].split(",");	
					if (properties.length == propCount) {
						for (int idx = 0; idx < properties.length; idx++) {
							if (maxProperties[idx] != 0) {	
								score += Double.parseDouble(properties[idx]) / maxProperties[idx] * weights[idx + similaties.length];
							}
						}					
					}else {
						String msg = String.format("user[%s]'s rec record[%s]'s properties length is not equal to configuration property.count[%d]", userId, pair.getFirst().toString(), propCount);
						throw new RuntimeException(msg);
					}				
					String result = userId + "|" + fields[0] + "|" + fields[1] + "|" + score;
					context.write(NullWritable.get(), new Text(result));				
				}	
			}
		}		
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		
		simCount = conf.getInt("select.similarity.count",1);
		propCount = conf.getInt("select.property.count", 1);
		String[] strWeights = conf.get("weights").split(",");//weight是必须的		
		weights = new double[strWeights.length];
		for (int idx = 0; idx < strWeights.length; idx++) {
			weights[idx] = Double.parseDouble(strWeights[idx]);
		}
		if (simCount + propCount != weights.length) {
			String msg = String.format("the sum of configuration similarty.count[%d] and configuration property.count[%d] is not equal to configuration weights's length[%d] ", simCount, propCount, weights.length);
			throw new RuntimeException(msg);
		}
	}
}
