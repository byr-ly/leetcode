package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenerateScoreMatrixReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	private Double downloadWeight;
	private Double collectWeight;
	private Double onlineplayWeight;
	private Double clickWeight;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double score = 0;
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			if (fields.length == 2) {
				String type = fields[0];
				long count = 0;
				try{
					count = Long.parseLong(fields[1]);
				} catch (Exception e) {
					count = 0;
					e.printStackTrace();
				}
				if (type.equals("0")) {
					score += downloadWeight * Math.log(count + 1);
				} else if (type.equals("1")) {
					score += collectWeight * Math.log(count + 1);
				} else if (type.equals("2")) {
					score += onlineplayWeight * Math.log(count + 1);
				} else if (type.equals("3")) {
					score += clickWeight * Math.log(count + 1);
				}
			}
		}
		context.write(new Text(key + "|" + score), NullWritable.get());
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration config = context.getConfiguration();
		downloadWeight = Double.parseDouble(config.get("downloadWeight"));
		collectWeight = Double.parseDouble(config.get("collectWeight"));
		onlineplayWeight = Double.parseDouble(config.get("onlineplayWeight"));
		clickWeight = Double.parseDouble(config.get("clickWeight"));
	}

}
