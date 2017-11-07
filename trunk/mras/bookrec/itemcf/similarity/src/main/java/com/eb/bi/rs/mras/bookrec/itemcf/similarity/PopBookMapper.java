package com.eb.bi.rs.mras.bookrec.itemcf.similarity;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


/**
 * 选取流行图书
 */
public class PopBookMapper extends Mapper<Object, Text, Text, NullWritable> {

	long bookUcAllTenthNum = 0;
	long orderUcTenthNum = 0;
	long bookUcTenthNum = 0;

	/**
	 * @param value 格式：图书|阅读用户数|订购用户数|深度阅读用户数
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");
		if (fields.length != 4) {
			return;
		}

		// 流行图书是指各项指标都大于等于对应指标的十分位数的图书
		long bookUcAll = Long.parseLong(fields[1]);
		if (bookUcAll < bookUcAllTenthNum) {
			return;
		}

		long orderUc = Long.parseLong(fields[2]);
		if (orderUc < orderUcTenthNum) {
			return;
		}

		long bookUc = Long.parseLong(fields[3]);
		if (bookUc < bookUcTenthNum) {
			return;
		}

		context.write(new Text(value), NullWritable.get());
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		Map<String, Long> tenthMap = new HashMap<String, Long>();

		for (Path path : localFiles) {
			BufferedReader br = null;
			try {
				String file = path.toString();
				br = new BufferedReader(new FileReader(file));
				String line;
				while ((line = br.readLine()) != null) {
					String[] fields = line.split("\\|");
					if (fields.length != 2) {
						continue;
					}
					tenthMap.put(fields[0], Long.parseLong(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}

		bookUcAllTenthNum = tenthMap.containsKey("book_uc_all") ? tenthMap.get("book_uc_all") : 0;
		orderUcTenthNum = tenthMap.containsKey("order_uc") ? tenthMap.get("order_uc") : 0;
		bookUcTenthNum = tenthMap.containsKey("book_uc") ? tenthMap.get("book_uc") : 0;
	}
}
