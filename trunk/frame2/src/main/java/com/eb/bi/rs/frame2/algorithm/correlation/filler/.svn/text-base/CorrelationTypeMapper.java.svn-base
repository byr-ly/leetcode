package com.eb.bi.rs.frame2.algorithm.correlation.filler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CorrelationTypeMapper extends Mapper<Object, Text, Text, Text> {

	private Map<String , String> classify_type = new HashMap<String, String>();
	private Set<String> rec_ids = new HashSet<String>();
	private String filler_subject;
	private String type_index;

	@Override
	protected void map(Object key, Text value,Context context) throws IOException, InterruptedException {
		
		String[] fields = value.toString().split("\t");
		String[] items = fields[1].split("\\|");
		if (fields.length == 2 ) {
			if (classify_type.keySet().contains(fields[0]) && rec_ids.contains(fields[0])) {
				String[] types = classify_type.get(fields[0]).split(",");
				for (String type : types) {
					context.write(new Text(type), new Text(fields[0] + "|" + items[0]));					
				}
				context.write(new Text(filler_subject), new Text(fields[0] + "|" + items[0]));
			}
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		filler_subject = configuration.get("filler.subject");
		type_index = configuration.get("type_index");
		int index = Integer.parseInt(type_index);
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				Path path = new Path(localFiles[i].getPath());
				in = new BufferedReader(new FileReader(path.getName().toString()));
				while((line = in.readLine()) != null) { 
					String[] fields = line.split("\\|");
					if (fields.length >= 6) {
						classify_type.put(fields[0], fields[index]);
						rec_ids.add(fields[0]);
					}					
				}				
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}

	}

}
