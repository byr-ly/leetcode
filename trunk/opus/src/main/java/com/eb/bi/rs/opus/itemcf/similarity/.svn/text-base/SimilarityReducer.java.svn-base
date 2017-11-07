package com.eb.bi.rs.opus.itemcf.similarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算动漫两两之间相似度
 */
public class SimilarityReducer extends
		Reducer<Text, SimilarityWritable, Text, Text> {

	Map<String, Float> opusModuleMap = new HashMap<String, Float>();

	@Override
	protected void reduce(Text key, Iterable<SimilarityWritable> values,
			Context context) throws IOException, InterruptedException {

		int count = 0;
		double sumProduct = 0;
		double moduleI = 0;
		double moduleJ = 0;
		String[] fields = key.toString().split("\\|");
		String I = fields[0];
		String J = fields[1];
		for (SimilarityWritable value : values) {
			count++;
			sumProduct += value.getProductij();
		}
		moduleI = opusModuleMap.get(I);
		moduleJ = opusModuleMap.get(J);
		double module = moduleI * moduleJ;
		if (sumProduct > 0 && module > 0) {
			double sim = sumProduct / module;
			context.write(key, new Text(String.format("%f|%d", sim, count)));
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		URI[] localFiles = context.getCacheFiles();
		for (URI localFile : localFiles) {
			String line = null;
			BufferedReader br = null;
			try {
				Path path = new Path(localFile.getPath());
				br = new BufferedReader(new FileReader(path.getName()));
				while ((line = br.readLine()) != null) {
					String fields[] = line.split("\\|");
					if (fields.length < 2) {
						continue;
					}
					opusModuleMap.put(fields[0], Float.parseFloat(fields[1]));
				}
			} finally {
				if (br != null) {
					br.close();
				}
			}
		}
	}

}
