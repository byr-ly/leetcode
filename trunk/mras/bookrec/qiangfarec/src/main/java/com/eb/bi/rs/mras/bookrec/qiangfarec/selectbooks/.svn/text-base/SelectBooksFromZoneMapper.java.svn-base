package com.eb.bi.rs.mras.bookrec.qiangfarec.selectbooks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SelectBooksFromZoneMapper extends Mapper<Object, Text, Text, NullWritable>  {
	private Map<String,String> bookInfo = new HashMap<String,String>();
	private Set<String> books = new HashSet<String>();
	
	@Override
	protected void map(Object key, Text value,Context context) throws IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|");//专区id | bookid |bu_type事业部| if_rec是否强推| if_on在当前专区|ontime 上专区时间
		if (fields.length == 6) {	
			String zoneId = fields[0];
			String bookId = fields[1];
			String bu_type = fields[2];
			String if_rec = fields[3];
			String if_on = fields[4];
			if ("1".equals(if_rec) && "1".equals(if_on)) {	//直接筛选专区图书中的强推图书
				if (bookInfo.containsKey(bookId)) {
					books.add(zoneId + "|" + bookId + "|" + bu_type + "|" + bookInfo.get(bookId) + "|");				
				} 
			}			
		}
	}
	
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);		
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				while((line = in.readLine()) != null) { //bookid图书 |分类Class_id					
					String[] fields = line.split("\\|", -1);
					if (fields.length == 2) {
						bookInfo.put(fields[0] , fields[1]);						
					}					
				}			
			}finally {
				if(in != null){
					in.close();
				}
			}			
		}
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		Iterator iter = books.iterator();
		while (iter.hasNext()) {
				context.write(new Text(iter.next().toString()), NullWritable.get());				
		}	
	}
}
