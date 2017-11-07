package com.eb.bi.rs.andedu.predictScore;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算目的品牌预测打分
 */
public class PredictScoreReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Double scores = 0.0;
		for (Text value : values) {
			scores +=  Double.parseDouble(value.toString());
		}

		String result = key.toString()+"|"+scores;
        context.write(new Text(result),new Text());
		
	}
}

