package com.eb.bi.rs.frame2.algorithm.correlation.edufilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 输入数据格式: key:源id value:id1,score1|id2,score2|... 
 * 缓存map的key-value: key:资源id   value:学科 
 * 输出数据格式: key:源id;资源1,score1|资源2,score2|...
 * 			  value:null
 */
public class EduCorrelationFilterReducer extends
		Reducer<Text, Text, Text, NullWritable> {
	// 存放资源id和科目的map
	Map<String, String> resInfoMap = new HashMap<String, String>();
	private int filterCondition;

	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder("");
		for (Text t : value) {
			String[] split = t.toString().split("\\|");
			for (String s : split) {
				String[] split2 = s.split(",");
				if (split2.length == 2 && StringUtils.isNotBlank(split2[0])
						&& resInfoMap.containsKey(key.toString())
						&& resInfoMap.containsKey(split2[0])) {
					String keySubject = resInfoMap.get(key.toString());
					String resultSubject = resInfoMap.get(split2[0]);
					if (keySubject.equals(resultSubject)) {
						sb.append(split2[0] + "," + split2[1] + "|");
					}
				}
			}
		}
		context.write(new Text(key.toString() + ";" + sb.toString()),
				NullWritable.get());
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = context.getCacheFiles();
		filterCondition = conf.getInt("filter.condition", 5);

		for(int i = 0; i < cacheFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(cacheFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				while((line = in.readLine()) != null) {
					/*
					 * resource_id资源ID|resource_score品质分|resource_type资源类型|
					 * content_type内容类型|
					 * phase学段|subject学科|version版本|grade年级|
					 * term分册|content目录（1级id，2级id，...）
					 */
					String[] fields = line.split("\\|");
					if (fields.length >= 10) {
						String bookId = fields[0]; // resource_id
						String subject = fields[filterCondition]; // subject学科
						resInfoMap.put(bookId, subject);
					}
				}				
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}
	}
}
