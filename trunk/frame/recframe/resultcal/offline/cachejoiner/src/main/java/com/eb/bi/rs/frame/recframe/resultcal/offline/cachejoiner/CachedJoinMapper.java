package com.eb.bi.rs.frame.recframe.resultcal.offline.cachejoiner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CachedJoinMapper extends Mapper<Object, Text, Text, NullWritable> {

	private Map<String, List<String>> cacheMap = new HashMap<String, List<String>>();
	private String[] fileStrs = null;
	private String[] cacheStrs = null;
	private int cacheKeyIndex;
	private int inputKeyIndex;
	private String cacheDelimiter;
	private String inputDelimiter;
	private String outputDelimiter;
	private String outputOrderDelimiter;
	private String outputOrder;
	private boolean isInputFirst;
	private String file1Field;
	private String file2Field;

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {

		fileStrs = value.toString().split(inputDelimiter, -1);
		if (fileStrs.length <= inputKeyIndex || !cacheMap.containsKey(fileStrs[inputKeyIndex])) {
			return;
		}

		String key = fileStrs[inputKeyIndex];
		Text keyOut = new Text(key);
		if (outputOrder != null) { // 有设置输入顺序
			// 把outputOrder以outputOrderDelimiter分割，表示输出顺序
			String[] result = outputOrder.split(outputOrderDelimiter);
			if (result.length < inputKeyIndex) {
				return;
			}

			List<String> cacheVals = cacheMap.get(key);
			// f2或c2
			String fileField;
			int index;
			String text = "";
			for (String cacheVal : cacheVals) {
				String[] cacheStrs = cacheVal.split(cacheDelimiter, -1);
				for (String res : result) {
					fileField = res.substring(0, 1);
					index = Integer.parseInt(res.substring(1));
					if (file1Field.equalsIgnoreCase(fileField)) { // 表示该字段来自文件1中
						if (index >= fileStrs.length) return;
						text += fileStrs[index] + outputDelimiter;
					} else if (file2Field.equalsIgnoreCase(fileField)) { // 表示该字段来自文件2中
						if (index >= cacheStrs.length) return;
						text += cacheStrs[index] + outputDelimiter;
					}
				}
				context.write(new Text(text.substring(0, text.length() - outputDelimiter.length())), NullWritable.get());
				System.out.println(text.substring(0, text.length() - outputDelimiter.length()));
				text = "";
			}
		} else { // 没有设置输入顺序,则按照 输入文件中的字段是否放在cache文件中的字段前面
			String lvalue = join(fileStrs, inputKeyIndex);
			List<String> rvalues = cacheMap.get(key);
			if (isInputFirst) {
				for (String rvalue : rvalues) {
					Text output = new Text(keyOut.toString() + outputDelimiter + lvalue + outputDelimiter + rvalue);
					context.write(output, NullWritable.get());
				}
			} else {
				for (String rvalue : rvalues) {
					Text output = new Text(keyOut.toString() + outputDelimiter + rvalue + outputDelimiter + lvalue);
					context.write(output, NullWritable.get());
				}
			}
		}

	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();

		String s = conf.get("conf.cache.key.index");
		cacheKeyIndex = Integer.parseInt(s);

		s = conf.get("conf.input.key.index");
		inputKeyIndex = Integer.parseInt(s);

		cacheDelimiter = conf.get("conf.cache.delimiter", "\\|");
		inputDelimiter = conf.get("conf.input.delimiter", "\\|");
		outputDelimiter = conf.get("conf.output.delimiter", "|");
		outputOrderDelimiter = conf.get("conf.output.order.delimiter", ",");
		file1Field = conf.get("conf.output.file1.field", "f");
		file2Field = conf.get("conf.output.file2.field", "c");
		isInputFirst = "true".equalsIgnoreCase(conf.get("conf.output.input.first", "true"));
		outputOrder = conf.get("conf.output.order");

		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		System.out.println("localFiles... " + localFiles.length);
		
		if (outputOrder != null) { // 有设置输入顺序
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					System.out.println("file... " + file);
					br = new BufferedReader(new FileReader(file));
					String line = null;
					while ((line = br.readLine()) != null) {
						cacheStrs = line.split(cacheDelimiter);
						if (cacheStrs.length <= cacheKeyIndex) {
							continue;
						}
						String key = cacheStrs[cacheKeyIndex];
						if (cacheMap.containsKey(key)) {
							cacheMap.get(key).add(line);
						} else {
							List<String> list = new ArrayList<String>();
							list.add(line);
							cacheMap.put(key, list);
						}
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}
		} else {
			for (Path path : localFiles) {
				BufferedReader br = null;
				try {
					String file = path.toString();
					br = new BufferedReader(new FileReader(file));
					String line;
					String[] fields = null;
					while ((line = br.readLine()) != null) {
						fields = line.split(cacheDelimiter);
						if (fields.length <= cacheKeyIndex) {
							continue;
						}

						String key = fields[cacheKeyIndex];

						String val = join(fields, cacheKeyIndex);

						if (cacheMap.containsKey(key)) {
							cacheMap.get(key).add(val);
						} else {
							List<String> list = new ArrayList<String>();
							list.add(val);
							cacheMap.put(key, list);
						}
					}
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}
		}
		
	}

	/**
	 * @param fields
	 *            要合并的字段
	 * @param excludeIndex
	 *            不包含的字段
	 */
	private String join(String[] fields, int excludeIndex) {

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < fields.length; i++) {
			if (i != excludeIndex) {
				sb.append(fields[i] + outputDelimiter);
			}
		}

		String ret = "";
		int end = sb.length();
		if (end > 0) {
			ret = sb.substring(0, end - outputDelimiter.length());
		}
		return ret;
	}
}
