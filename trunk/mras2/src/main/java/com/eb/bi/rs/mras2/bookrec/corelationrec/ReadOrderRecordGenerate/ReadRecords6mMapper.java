package com.eb.bi.rs.mras2.bookrec.corelationrec.ReadOrderRecordGenerate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadRecords6mMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private HashSet<String> cartoons;
	
   @Override
   protected void map(Object key, Text value, Context context)throws IOException, InterruptedException
   {
	 //用户msisdn|book_id图书|read_depth|tele_type号码类型|chrg_dmd_fee点播费用|dnld_cnt下载量|vst_pv访问量|last_read_day最近阅读时间
     String[] fields = value.toString().split("\\|");
     if(!cartoons.contains(fields[1])){
    	 context.write(new Text(fields[0]+"|"+fields[1]), NullWritable.get());
     }
    
   }
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException 	{
		
		Configuration conf = context.getConfiguration();
		String cartoonInfoPath = conf.get("cartoon.info.path");
		
		cartoons = new HashSet<String>();
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {			
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if (localFiles[i].toString().contains(cartoonInfoPath)) {
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						if(fields.length >=2) {					
							cartoons.add(fields[0]);						
						}					
					}					
				} 
			} finally{
				if(in != null){
					in.close();
				}				
			}		
			
		}
		
	}
 }