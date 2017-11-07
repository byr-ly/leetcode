package com.eb.bi.rs.qhll.userapprec.predictpref;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemNameReplaceMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	private final static HashMap<String, String> itemNameIdMap = new HashMap<String, String>();
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException 
	{
		super.setup(context);
		
		Configuration conf = context.getConfiguration();	  
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; i++)
		{
			String line;
			BufferedReader in = null;
			try 
			{
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while ((line = in.readLine()) != null) 
				{
					String[] fields = line.split("\t");
					itemNameIdMap.put(fields[1], fields[0]);
				}
			} 
			finally 
			{
				if (in != null) 
				{
					in.close();
				}
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String fields[] = value.toString().split("\\|");
		
		if(fields.length != 3 )
		{
			return;
		}
		
		String result = itemNameIdMap.get(fields[1]) + "\t" + fields[2];
		context.write(new Text(fields[0]), new Text(result));
	}
}

