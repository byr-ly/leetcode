package com.eb.bi.rs.frame2.service.dataload.unifyrecs2hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersonalTailorNewMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String, String> cache = new HashMap<String, String>();
	private int index;

	@Override
	public void map(Object o, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split("\\|");

		int len = fields.length;
		for (int i = 1; i < len; i++) {
			fields[i] = fields[i] + "," + cache.get(fields[i]);
			context.write(new Text(fields[0]), new Text(fields[i]));
		}
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		index = conf.getInt("conf.online.day.index", 2);

		URI[] localFiles = context.getCacheFiles();
		for (URI localFile : localFiles) {
			BufferedReader br = null;
			try {
                Path path = new Path(localFile.getPath());
                br = new BufferedReader(new FileReader(path.getName().toString()));
                String line;
                String[] fields = null;
                while ((line = br.readLine()) != null) {
                    fields = line.split("\\|", -1);
                    System.out.println("fields[0]: " + fields[0] + "fields[2]: " + fields[index]);
                    /*
					 * if (fields.length <= index + 1) { continue; }
					 */
                    cache.put(fields[0], fields[index]);
                }
            } finally {
				if (br != null)
					br.close();
			}
		}
	}
}
