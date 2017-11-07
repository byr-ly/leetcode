package com.eb.bi.rs.frame2.algorithm.correlation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataPrepReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,Context context)
			throws IOException, InterruptedException {
		StringBuffer ids = new StringBuffer();
		List<String> idlist = new ArrayList<String>();
		for (Text text : value) {
			idlist.add(text.toString());
		}
		if (idlist.size() > 1) {
			Collections.sort(idlist);
		}
		for(int i = 0; i < idlist.size(); i++){
			ids.append(idlist.get(i)+"|");
		}
		context.write(key, new Text(ids.toString()));
		context.getCounter("RECORD", "NUM").increment(1);
	}

}
