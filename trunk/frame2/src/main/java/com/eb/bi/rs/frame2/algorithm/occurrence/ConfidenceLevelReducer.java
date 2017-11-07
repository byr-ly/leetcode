package com.eb.bi.rs.frame2.algorithm.occurrence;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ConfidenceLevelReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String,Float> booknumList = new TreeMap<String,Float>();
	private float KULC_threshold;
	private float IR_threshold;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		KULC_threshold = Float.valueOf(context.getConfiguration().get("KULC_threshold"));
		IR_threshold = Float.valueOf(context.getConfiguration().get("IR_threshold"));
		
		super.setup(context);

		/*DistributedCache修改点*/
		URI[] localFiles = context.getCacheFiles();
//		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());		

		for(int i = 0; i < localFiles.length; i++){
			String line;
			BufferedReader in = null;
			try {
                /*DistributedCache修改点*/
                Path path = new Path(localFiles[i].getPath());
                in = new BufferedReader(new FileReader(path.getName().toString()));
//				in = new BufferedReader(new FileReader(localFiles[i].toString()));

                while ((line = in.readLine()) != null) {
                    String fields[] = line.split("\\|");

                    if (fields.length > 2)
                        continue;

                    booknumList.put(fields[0], Float.valueOf(fields[1]));
                }

            } finally {
				if (in != null) {
					in.close();
				}			
			}			
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//搜索key(itemAid)的num(Anum)
		
		float Anum = 0;
		float Bnum = 0;
		
		float AB_A = 0;
		float AB_B = 0;
		
		float KULC = 0;
		float IR = 0;
		
		int ClassType = 0;
		
		Anum = booknumList.get(key.toString());
		
		for (Text value : values){
			String[] bookidInfo;
			//bookidInfo[0](itemBid)的num(Bnum),bookidInfo[1](ABcomnum)
			bookidInfo = value.toString().split("\\|");
			
			Bnum = booknumList.get(bookidInfo[0]);
			
			AB_A = Float.valueOf(bookidInfo[1])/Anum;
			AB_B = Float.valueOf(bookidInfo[1])/Bnum;
			KULC = (AB_A + AB_B)/2;
			IR   = AB_A / AB_B;
			
			if(KULC > KULC_threshold && IR> IR_threshold)
				ClassType = 1;
			else
				ClassType = 2;
			
			//key : itemAid | val : itemBid|Anum|Bnum|ABcomnum|AB/A|AB/B|KULC|IR|classtype
			context.write(key, new Text(bookidInfo[0] + "|" + Anum + "|" +Bnum+ "|" +bookidInfo[1] 
										+"|"+ AB_A +"|"+ AB_B +"|"+ KULC +"|"+ IR +"|"+ ClassType));
		}
	}
}
