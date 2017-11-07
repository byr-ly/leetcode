package com.eb.bi.rs.mras2.bookrec.channelrec.mergedata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeDataReducer extends Reducer<Text , Text, Text, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		Map<String, String> type_data = new HashMap<String, String>();
		
		for(Text value:values){
			
			String[] fields = value.toString().split(";");
			if (fields.length == 2) {
				type_data.put(fields[0], fields[1]);
			}
		}
		
		String allData = type_data.get(MergeAllDataMapper.datatype);
		String incrementData = type_data.get(MergeIncrementDataMapper.datatype);
		if (!StringUtils.isBlank(incrementData)) {
			context.write(new Text(incrementData), NullWritable.get());
		}else if (!StringUtils.isBlank(allData)) {
			context.write(new Text(allData), NullWritable.get());
		}
		
	}
}
