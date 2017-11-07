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

public class BookInfoMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private HashSet<String> cartoons;
	
   @Override
   protected void map(Object key, Text value, Context context)throws IOException, InterruptedException
   {
	 //bookid图书ID|authorid作者ID|classid分类ID|big_class大类ID|charge_type计费类型|series_id系列号|order_id顺序号
     String[] fields = value.toString().split("\\|");
     if(!cartoons.contains(fields[0])){
    	 context.write(value, NullWritable.get());
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