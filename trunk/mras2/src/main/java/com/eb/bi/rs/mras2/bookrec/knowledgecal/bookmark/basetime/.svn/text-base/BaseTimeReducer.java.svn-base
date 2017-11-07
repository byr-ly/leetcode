package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark.basetime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BaseTimeReducer extends Reducer<Text, Text, NullWritable, Text> {

	private int h1 = 12;
	private int M = 60;

	private Date current;
	private SimpleDateFormat format;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// 生成当前计算时间
		current = new Date();

		format = new SimpleDateFormat("yyyyMMdd");

		h1 = Integer
				.valueOf(context.getConfiguration().get("hdfs.app.conf.h1"));

		M = Integer.valueOf(context.getConfiguration().get("hdfs.app.conf.M"));
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 用户优化后图书打分
		double newScore = 0.0;
		double exponent = 0.0;
		double oldScore = -1.0;
		Date lastTime = null;

		String info = "";

		// 数据分离
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");

			if (fields[0].equals("0")) {// 0|用户|图书|主要分|次要分|总分
				oldScore = Double.parseDouble(fields[5]);
				info = value.toString().substring(2);
			}

			if (fields[0].equals("1")) {// 1|最后一次访问时间YYYYMMDD
				try {
					lastTime = format.parse(fields[1]);
				} catch (ParseException e) {
					System.out.println(lastTime + " is not valid");

				}
			}
		}

		// 没有用户打分数据,跳过
		if (oldScore == -1.0) {
			return;
		}

		// 没有最后一次访问时间，默认值就用：当前日期-60天
		if (lastTime == null) {
			lastTime = new Date();
			lastTime.setTime(current.getTime() - 1000L * M * 60 * 24 * 60);
		}

		int dayInterval = (int) ((current.getTime() - lastTime.getTime()) / (24 * 60 * 60 * 1000));

		exponent = -Math.log(2) / h1 * (dayInterval >= M ? M : dayInterval);

		newScore = oldScore * Math.pow(Math.E, exponent);

		context.write(NullWritable.get(), new Text(info + "|" + newScore));
	}
}
