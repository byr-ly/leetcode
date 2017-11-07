package com.eb.bi.rs.andedu.combination;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntegrationReducer extends Reducer<Text, Text, Text, NullWritable> {
	protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
		String msisdn_id = key.toString();
		String visit = "0";
		String download = "0";
		String collection = "0";
		for (Text text : values) {
			String[] fields = text.toString().split("\\|");
			if ("0".equals(fields[0])) {
				visit = fields[1];
			}else if("1".equals(fields[0])){
				download = fields[1];
			}else if("2".equals(fields[0])){
				collection = fields[1];
			}
		}
		context.write(new Text(msisdn_id + "|" + visit + "|" + download + "|" + collection), NullWritable.get());
	}
}
