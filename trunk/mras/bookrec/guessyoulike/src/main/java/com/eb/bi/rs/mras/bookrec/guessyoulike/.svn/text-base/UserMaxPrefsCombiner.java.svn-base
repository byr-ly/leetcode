package com.eb.bi.rs.mras.bookrec.guessyoulike;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserMaxPrefsCombiner extends Reducer<Text, Text, Text, Text> {	
	private int simCount;
	private int propCount;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
		
		double[] maxSimilarities = new double[simCount];
		for (int idx = 0; idx < maxSimilarities.length; idx++) {
			maxSimilarities[idx] = Double.MIN_VALUE;
		}
		
		if (propCount != 0) {
			double[] maxProperties = new double[propCount];
			for (int idx = 0; idx < maxProperties.length; idx++) {
				maxProperties[idx] = Double.MIN_VALUE;
			}
			
			for (Text value : values ) {
				String[] field = value.toString().split("\\|",-1);
				if(field.length == 2) {
					String[] similarities = field[0].split(",");
					if (similarities.length == simCount) {
						for (int idx = 0; idx < similarities.length; idx++) {
							double tmp = Double.parseDouble(similarities[idx]);						
							if (tmp > maxSimilarities[idx]) {
								maxSimilarities[idx] = tmp;
							}
						}
					}
					String[] properties = field[1].split(",");
					if (properties.length == propCount) {
						for (int idx = 0; idx < properties.length; idx++) {
							double tmp = Double.parseDouble(properties[idx]);
							if (tmp > maxProperties[idx]) {
								maxProperties[idx] = tmp;
							}
						}					
					}
				}			
			}
			
			StringBuffer result = new StringBuffer();
			for (int idx = 0; idx < maxSimilarities.length; idx++) {
				if (idx != maxSimilarities.length -1) {
					result.append(maxSimilarities[idx] + ",");
				}else {
					result.append(maxSimilarities[idx] + "|");					
				}			
			}
			
			for (int idx = 0; idx < maxProperties.length; idx++) {
				if (idx != maxProperties.length -1) {
					result.append(maxProperties[idx] + ",");	
				}else {
					result.append(maxProperties[idx]);
				}			
			}		
			context.write(key, new Text(result.toString()));			
		} else {
			for (Text value : values ) {
				String[] similarities = value.toString().split(",");
				if (similarities.length == simCount) {
					for (int idx = 0; idx < similarities.length; idx++) {
						double tmp = Double.parseDouble(similarities[idx]);						
						if (tmp > maxSimilarities[idx]) {
							maxSimilarities[idx] = tmp;
						}
					}
				}					
			}
			StringBuffer result = new StringBuffer();
			for (int idx = 0; idx < maxSimilarities.length; idx++) {
				if (idx != maxSimilarities.length -1) {
					result.append(maxSimilarities[idx] + ",");
				}else {
					result.append(maxSimilarities[idx]);					
				}			
			}
			context.write(key, new Text(result.toString()));
		}

	}
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {

		Configuration conf = context.getConfiguration();
		simCount = conf.getInt("select.similarity.count",1);
		propCount = conf.getInt("select.property.count", 1);
		
		
	}
}
