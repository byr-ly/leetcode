package com.eb.bi.rs.opus.itemcf.predictscore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算用户预测topN评分的动漫
 */
public class TopUserPredictOpusReducer extends Reducer<Text, Text, Text, Text> {

	int topN = 0;
	private Vector<String> rankSet = new Vector<String>();
	private Vector<String> saVector = new Vector<String>(); // SA级
	/**
	 * key 格式：用户 value 格式：动漫|预测评分
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Float> map = new HashMap<String, Float>();
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			String opus = fields[0];
			if(saVector.contains(opus)){
				map.put(opus, Float.parseFloat(fields[1]));
			}
		}
		List<Map.Entry<String, Float>> infoIds = new ArrayList<Map.Entry<String, Float>>(
				map.entrySet());
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Float>>() {

			@Override
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		int count = 0;
		String opusScore = "";
		for (int i = 0; i < infoIds.size() && count < topN; i++) {
			opusScore += infoIds.get(i).getKey() + ","
					+ infoIds.get(i).getValue() + "|";
			count++;
		}
		context.write(new Text(key.toString()), new Text(opusScore));

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		topN = Integer
				.parseInt(conf.get("conf.top.userpredictscore.num", "50"));
		String[] rankStr = conf.get("ranks").split(",");
		for (String rank : rankStr) {
			rankSet.add(rank);
		}
		
		URI[] cacheFiles = context.getCacheFiles();
		String line = null;
		BufferedReader in = null;
		String opusId = null;
		String rank = null;
		for (URI cacheFile : cacheFiles) {
			try {
				Path path = new Path(cacheFile.getPath());
				in = new BufferedReader(new FileReader(path.getName()
						.toString()));
				while ((line = in.readLine()) != null) {
					String[] fields = line.split("\\|");
					if (fields.length == 7) {
						opusId = fields[0];
						rank = fields[4];
						if (rankSet.contains(rank)) { // SA级
							saVector.add(opusId);
						}
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}
	}

}
