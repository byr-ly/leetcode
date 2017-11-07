package com.eb.bi.rs.andedu.individuation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 输入数据格式:
 * 			key:用户id
 * 			value:资源1,推荐分1|资源2,推荐分2|...100|
 * 缓存map的key-value:
 * 			key:资源id   value:资源品质分
 * 输出数据格式:
 * 			key:用户id;资源1,个性化分1|资源2,个性化分2|...100
 *
 */
public class IndividuationRecReducer extends
		Reducer<Text, Text, Text, NullWritable> {
	private int weightNum;
	//存放资源id和品质分的map
	Map<String, String> resInfoMap = new HashMap<String, String>();
	
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		//存放id和预测分的map
		Map<String, String> forecastScoreMap = new HashMap<String, String>();
		//存放个性化分的map
		Map<String, Float> commonMap = new HashMap<String, Float>();

		for(Text t : value){
			String[] split = t.toString().split("\\|");
			for(String s :split){
				String[] split2 = s.split(",",-1);
				if (split2.length == 2) {
					forecastScoreMap.put(split2[0],split2[1]);
				}
			}
		}
		
		float indiviScore = 0 ;
		Iterator<Entry<String, String>> iterator = forecastScoreMap.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, String> next = iterator.next();
			String resinfoScore = resInfoMap.get(next.getKey());
			resinfoScore = StringUtils.isBlank(resinfoScore) ? "0" : resinfoScore;
			float resInfoScore = Float.parseFloat(resinfoScore);
			String temp_value = next.getValue();
			temp_value = StringUtils.isBlank(temp_value) ? "0" : temp_value;
			float forecastScore = Float.parseFloat(temp_value);
			DecimalFormat df = new DecimalFormat("0.00000");//格式化小数
			indiviScore = Float.parseFloat(df.format((float) (forecastScore+ (resInfoScore/weightNum) )));
			commonMap.put(next.getKey(), indiviScore);
		}
		
		StringBuffer sb = new StringBuffer("");
		Map<String, Float> sortMapByValue = sortMapByValue(commonMap);
		
		for (Entry<String, Float> entry : sortMapByValue.entrySet()) {
			sb.append(entry.getKey()+","+entry.getValue()+"|");
		}
        
        if (StringUtils.isNotBlank(sb.toString())) {
        	String substring = sb.toString().substring(0, sb.toString().length()-1);
        	context.write(new Text(key.toString()+";"+substring), NullWritable.get());
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = context.getCacheFiles();
		weightNum = conf.getInt("weight.num", 100);
		String cacheInputPath = conf.get("cache.input.path");

		for (int i = 0; i < cacheFiles.length; i++) {
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(cacheFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(
						cacheFiles[i]))));
				if (cacheFiles[i].toString().contains(cacheInputPath)) {
					while ((line = in.readLine()) != null) {
						/*
						 * resource_id资源ID|resource_score品质分|resource_type资源类型|content_type内容类型|
						 * phase学段|subject学科|version版本|grade年级|term分册|content目录（1级id，2级id，...）
						 */
						String[] fields = line.split("\\|", -1);
						if (fields.length >= 10) {
							String bookId = fields[0]; // resource_id
							String classid = fields[1]; // resource_score品质分
							resInfoMap.put(bookId, classid);
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
	//map按value排序
		public Map<String, Float> sortMapByValue(Map<String, Float> oriMap) {
			Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
			if (oriMap != null && !oriMap.isEmpty()) {
				List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(oriMap.entrySet());
				Collections.sort(entryList,
						new Comparator<Map.Entry<String, Float>>() {
							@Override
							public int compare(Entry<String, Float> entry1,
									Entry<String, Float> entry2) {
								float value1 = 0, value2 = 0;
								try {
									value1 = entry1.getValue();
									value2 = entry2.getValue();
								} catch (NumberFormatException e) {
									value1 = 0;
									value2 = 0;
								}
								if (value2 > value1) {
									return 1;
								} else if (value2 < value1) {
									return -1;
								} else if ((value2 - value1) < 0.001) {
									return 0;
								} else {
									return 0;
								}

							}
						});
				Iterator<Map.Entry<String, Float>> iter = entryList.iterator();
				Map.Entry<String, Float> tmpEntry = null;
				while (iter.hasNext()) {
					tmpEntry = iter.next();
					sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
				}
			}
			return sortedMap;
		}	
}
