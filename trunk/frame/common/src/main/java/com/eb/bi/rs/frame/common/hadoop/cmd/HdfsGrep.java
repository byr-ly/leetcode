package com.eb.bi.rs.frame.common.hadoop.cmd;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 一个简单grep程序，可从文档中提取包含子串的行
 */
public class HdfsGrep extends Configured implements Tool {

	public static class grepMap extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		public void map(LongWritable line, Text value, Context context)
				throws IOException, InterruptedException {
			// 通过Configuration获取参数
			String str = context.getConfiguration().get("grep");
			if (value.toString().contains(str)) {
				context.write(value, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HdfsGrep(), args);
		System.exit(ret);
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("ERROR");
			System.exit(1);
		}

		Configuration configuration = getConf();
		// 传递参数
		configuration.set("grep", args[2]);
		Job job = new Job(configuration, "grep");

		job.setJarByClass(HdfsGrep.class);
		job.setMapperClass(grepMap.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileSystem fileSystem = out.getFileSystem(configuration);
		if (fileSystem.exists(out))
			fileSystem.delete(out, true);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
