package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


/**
 * 计算单个用户对两本图书之间相似度的贡献
 */
public class SimilarityMapper extends Mapper<Object, Text, Text, SimilarityWritable> {

	// 流行图书的权重
	Map<String, Double> weightMap = new HashMap<String, Double>();

	/**
	 * @param value 格式：图书I|图书J|评分I|评分J
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 4) {
			return;
		}

		String i = fields[0];
		String j = fields[1];
		double rui = Double.parseDouble(fields[2]);
		double ruj = Double.parseDouble(fields[3]);
		double wi = weightMap.get(i);
		double wj = weightMap.get(j);

		double wr = wi * rui * wj * ruj;
		double ri = rui * rui;
		double rj = ruj * ruj;

		context.write(new Text(i + "|" + j), new SimilarityWritable(wr, ri, rj));
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
					weightMap.put(fields[0], Double.parseDouble(fields[2]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}
}
