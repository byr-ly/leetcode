package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class OnlineDayTool extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator", "|");
		// 是否对新书推荐结果中用户ID进行更改（增加事业群类型）
		conf.set("conf.input.replacekey", properties.getProperty("conf.input.replacekey", "false"));
		// 对新书推荐结果中用户ID更改后缀（添加事业群编号）
		conf.set("conf.input.replacekey.flag", properties.getProperty("conf.input.replacekey.flag", ""));

		String index = properties.getProperty("conf.online.day.index", "2");
		conf.set("conf.online.day.index", index);

		String cachePath = properties.getProperty("conf.book.info.path");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.globStatus(new Path(cachePath));
		for (FileStatus st : status) {
			DistributedCache.addCacheFile(URI.create(st.getPath().toString()), conf);
		}

		Job job = new Job(conf, "add online day");
		job.setJarByClass(OnlineDayTool.class);
		job.setMapperClass(OnlineDayMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);
		String inputPath = properties.getProperty("conf.input.path");
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		String outputPath = properties.getProperty("conf.output.path");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		check(outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public void check(String fileName) {
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
			Path f = new Path(fileName);
			if (fs.exists(f)) {
				fs.delete(f, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static class OnlineDayMapper extends Mapper<Object, Text, Text, NullWritable> {

		private Map<String, String> cache = new HashMap<String, String>();
		private int index;

		@Override
		public void map(Object o, Text value, Context context) throws IOException, InterruptedException {
	
			String[] fields = value.toString().split("\\|");
			// 替换用户ID为ID+事业部编号
			Configuration conf = context.getConfiguration();
			if (conf.get("conf.input.replacekey").equals("true")) {
				fields[0] = fields[0] + conf.get("conf.input.replacekey.flag");
			}

			int len = fields.length;
			for (int i = 1; i < len; i++) {
				fields[i] = fields[i] + "," + cache.get(fields[i]);
			}

			String keyOut = join(fields);
			context.write(new Text(keyOut), NullWritable.get());
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);

			index = context.getConfiguration().getInt("conf.online.day.index", 2);


			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split("\\|");
						if (fields.length <= index) {
							continue;
						}

						cache.put(fields[0], fields[index]);
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}
		}

		/**
		 * 将数组中元素连接成字符串，以"|"分隔
		 * @param fields 需要连接的字符串数组
		 */
		private String join(String[] fields) {

			StringBuffer sb = new StringBuffer();
			for (String field : fields) {
				sb.append(field + "|");
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}

			return sb.toString();
		}
	}

} 
