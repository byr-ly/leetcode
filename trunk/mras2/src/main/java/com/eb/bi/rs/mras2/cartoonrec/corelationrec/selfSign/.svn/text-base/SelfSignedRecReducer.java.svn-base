package com.eb.bi.rs.mras2.cartoonrec.corelationrec.selfSign;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SelfSignedRecReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	// 存放图书分类的map id class_id
	private HashMap<String, String> classidMap = new HashMap<String, String>();
	// 存放图书是否是自签书的map id 是否自签书 1:是 0:不是
	private HashMap<String, String> selfSignMap = new HashMap<String, String>();
	// 存放分类和大类信息的map class_id | id1 id2 id3
	private HashMap<String, ArrayList<String>> classidAndbigMap = new HashMap<String, ArrayList<String>>();
	// 推荐结果存放的List
	private LinkedList<BookInfo> recRescultList = new LinkedList<BookInfo>();
	// 存放推荐结果和补白里面的自签书
	private LinkedList<BookInfo> selfSignList = new LinkedList<BookInfo>();
	//过滤掉相同的自签书
	ArrayList<String> selfSignFiller = new ArrayList<String>();
	
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		// 用于结果的拼接
		StringBuilder sb = new StringBuilder("");
		recRescultList.clear();
		selfSignList.clear();
		selfSignFiller.clear();
		
		// 将推荐结果存到recRescultList中
		for (Text t : value) {
			String[] split = t.toString().split("\\|", -1);
			for (String s : split) {
				String[] split1 = s.split(",", -1);
				if (split1.length == 2) {
					recRescultList.add(new BookInfo(split1[0], split1[1]));
				}
			}
		}
		// 从推荐结果里面挑出自签书存放到selfSignList里面
		for (BookInfo b : recRescultList) {
			// System.out.println(b.getId() + "  haha   " + b.getScore());
			if (selfSignMap.containsKey(b.getId())
					&& selfSignMap.get(b.getId()).equals("1")) {
				selfSignList.add(b);
				selfSignFiller.add(b.getId());
				if (selfSignList.size() == 4) {
					break;
				}
			}
		}

		// 推荐结果里自签书的数量
		int recResultSelfSignNum = selfSignList.size();

		// 移除recRescultList中的自签书
		for (BookInfo bi : selfSignList) {
			recRescultList.remove(bi);
		}

		// 第一次补白 分类补白
		if (selfSignList.size() < 4) {
			if (classidMap.containsKey(key.toString())) {
				String classid = classidMap.get(key.toString());

				if (classidAndbigMap.containsKey(classid)) {

					ArrayList<String> list = classidAndbigMap.get(classid);
					Collections.shuffle(list);
					for (String s : list) {
						if (!s.equals(key.toString())
								&& selfSignMap.get(s).equals("1") && !selfSignFiller.contains(s)) {
							selfSignList.add(new BookInfo(s, ""));
						}
						if (selfSignList.size() == 4) {
							break;
						}

					}
				}
			}
		}
		System.out.println("===================================");
		// 如果是补白的在日志中标记出来
		String keyStr = key.toString() + ";";
		for (BookInfo b : selfSignList) {
			if (b.getScore().equals("")) {
				keyStr = keyStr + b.getId() + " is filler|";
			}
		}
		System.out.println(keyStr);
		System.out.println("===================================");
		// 补白的自签书的数量
		int fillerNum = selfSignList.size() - recResultSelfSignNum;

		int i = 0;
		int j = fillerNum;
		int z = fillerNum;

		if (recResultSelfSignNum == 0 && selfSignList.size() == 0) {

			// 无自签书 按源数据输出
			for (int q = 0; q < 50; q++) {

				sb = sb.append(recRescultList.get(q).getId() + "|"
						+ recRescultList.get(q).getScore() + "|");

			}

			String sbString = sb.toString();
			// String result = sbString.substring(0, sbString.length() - 1);
			context.write(new Text(key.toString() + "|" + sbString),
					NullWritable.get());

		} else {
			// 推荐结果里自签书的数量等于0时 a==0
			if (recResultSelfSignNum == 0) {
				for (BookInfo bi : selfSignList) {
					recRescultList.add(10 - j, bi);
					bi.setScore(recRescultList.get(10 - j - 1).getScore());
					j--;
				}
			}

			// 推荐结果里自签书的数量不等于0时 !a==0时
			if (!(recResultSelfSignNum == 0)) {
				// 根据a 和 b 的值来随机a个数
				int[] randomCommon = randomCommon(10 - fillerNum,
						recResultSelfSignNum);
				for (BookInfo bi : selfSignList) {
					if (i < recResultSelfSignNum) {
						recRescultList.add(randomCommon[i], bi);
						// 当随机数为0时,获取index为1的score
						if (randomCommon[i] == 0) {
							bi.setScore(recRescultList.get(1).getScore());
						} else {
							bi.setScore(recRescultList.get(randomCommon[i] - 1)
									.getScore());
						}
					}
					i++;
					if (i > recResultSelfSignNum) {
						recRescultList.add(10 - z, bi);
						bi.setScore(recRescultList.get(10 - z - 1).getScore());
						z--;
					}
				}

			}

			// 输出前50个
			for (int q = 0; q < 50; q++) {

				sb = sb.append(recRescultList.get(q).getId() + "|"
						+ recRescultList.get(q).getScore() + "|");

			}

			String sbString = sb.toString();
			// String result = sbString.substring(0, sbString.length() - 1);
			context.write(new Text(key.toString() + "|" + sbString),
					NullWritable.get());
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = context.getCacheFiles();
		String cacheInputPath = conf.get("cache.input.path");

		int selfIndex = conf.getInt("self.index", 5);
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
						 * bookid图书ID|authorid作者ID|classid分类ID|big_class大类ID|
						 * charge_type计费类型
						 * |series_id系列号|order_id顺序号|selfsign自签书|isUGC是否ugc
						 */
						String[] fields = line.split("\\|", -1);
						if (fields.length >= 6) {
							String bookId = fields[0]; // bookid
							String classid = fields[2]; // classid
							String selfSign = fields[selfIndex]; // selfSign
							classidMap.put(bookId, classid);
							selfSignMap.put(bookId, selfSign);
							if (classidAndbigMap.containsKey(classid)) {
								classidAndbigMap.get(classid).add(bookId);
							} else {
								classidAndbigMap.put(classid,
										new ArrayList<String>());
								classidAndbigMap.get(classid).add(bookId);
							}
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

	// 随机两个数
	// max : 10-a-b 随机数的最大值 范围是:[0,max)
	// n : a 随机数字的个数
	// 随机出来的数字不能重复且必须按照升序排序
	public static int[] randomCommon(int max, int n) {
		if (n > (max + 1)) {
			return null;
		}
		int[] result = new int[n];
		int count = 0;
		while (count < n) {
			int num = (int) ((Math.random() * max));
			boolean flag = true;
			for (int j = 0; j < n; j++) {
				if (num == result[j]) {
					if (num == 0 && count == 0) {
						result[count] = num;
						count++;
					}
					flag = false;
					break;
				}
			}
			if (flag) {
				result[count] = num;
				count++;
			}
		}
		Arrays.sort(result);
		return result;
	}
}
