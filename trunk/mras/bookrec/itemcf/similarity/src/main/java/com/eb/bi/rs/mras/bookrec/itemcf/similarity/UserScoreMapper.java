package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

/**
 * 过滤掉非流行图书的评分信息，并将用户评分信息按用户分组
 */
public class UserScoreMapper extends Mapper<Object, Text, Text, ScoreWritable> {

	private Set<String> popBooks = new HashSet<String>();

	/**
	 * @param value 格式：用户|图书|评分
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		// 过滤掉非流行图书的评分信息
		if (fields.length != 3 || !popBooks.contains(fields[1])) {
			return;
		}

		String user = fields[0];
		String bookId = fields[1];
		double score = Double.parseDouble(fields[2]);

		context.write(new Text(user), new ScoreWritable(bookId, score));
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);

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
					if (fields.length != 3) {
						continue;
					}
					popBooks.add(fields[0]);
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}
}
