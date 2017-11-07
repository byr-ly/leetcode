package com.eb.bi.rs.opus.itemcf.similarity.mark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GenerateScoreMatrixReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	private Double downloadWeight;
	private Double collectWeight;
	private Double onlineplayWeight;
	private Double clickWeight;
	private Vector<String> animationVector = new Vector<String>(); // 动画
	private Vector<String> cartoonVector = new Vector<String>(); // 漫画
	private Vector<String> rankSet = new Vector<String>();
	
	private String cartoon;
	private String comic;
	
	private String cartoonOutput;
	private String comicOutput;

	private MultipleOutputs<Text, NullWritable> mos;

	// key : 用户ID|动漫ID value : 行为|次数
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double score = 0;

		String opusId = key.toString().split("\\|")[1];
		for (Text value : values) {
			String[] fields = value.toString().split("\\|");
			if (fields.length == 2) {
				String type = fields[0];
				long count = 0;
				try {
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
		if (animationVector.contains(opusId)) {
			mos.write(new Text(key + "|" + score), NullWritable.get(),
					cartoonOutput);
		} else if (cartoonVector.contains(opusId)) {
			mos.write(new Text(key + "|" + score), NullWritable.get(),
					comicOutput);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// 初始化多输出
		mos = new MultipleOutputs<Text, NullWritable>(context);

		Configuration config = context.getConfiguration();
		downloadWeight = Double.parseDouble(config.get("downloadWeight"));
		collectWeight = Double.parseDouble(config.get("collectWeight"));
		onlineplayWeight = Double.parseDouble(config.get("onlineplayWeight"));
		clickWeight = Double.parseDouble(config.get("clickWeight"));
		cartoonOutput = config.get("cartoonOutput");
		comicOutput = config.get("comicOutput");
		cartoon = config.get("cartoon");
		comic = config.get("comic");

		String[] rankStr = config.get("ranks").split(",");
		for (String rank : rankStr) {
			rankSet.add(rank);
		}
		URI[] cacheFiles = context.getCacheFiles();

		String line = null;
		BufferedReader in = null;
		String opusId = null;
		String type = null;
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
						type = fields[1];
						rank = fields[4];
						if (cartoon.equals(type) /*&& rankSet.contains(rank)*/) { // 动画 & SA级
							animationVector.add(opusId);
						} else if (comic.equals(type) /*&& rankSet.contains(rank)*/) { // 漫画 & SA级
							cartoonVector.add(opusId);
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

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
