package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
	上面的模块执行完后，会有如下几种图书没有推荐列表：
	1.没有被订购或者与其他书一起被订购过的图书，即没有出现在订购关联关系中的图书
	2.出现在订购关联关系表中，但是所有记录都不满足 “支持度大于一定阈值且改善度大于一定阈值”的条件
	3.出现在订购关联关系表中，但是其关联图书都下架
	我们也需要为这些图书推荐关联图书。
*/

public class ReadRecForBookWithoutCooccurrenceInfoMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {	
	

	private HashSet<String> recommendedBookSet = new HashSet<String>();	
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {		
	   //动漫ID|作者ID|分类ID|计费类型|点击量
		String[] fields = value.toString().split("\\|",-1);		
		if(fields.length >=5){	
			String bookId = fields[0];
			if(!recommendedBookSet.contains(bookId)){//全部的书-3里已经有推荐结果的书
				context.write(new TextPair(value.toString(),"1"), new TextPair("","1"));				
			}	
		}	
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String recResultPath = conf.get("corelation.book.recommend.path");		
		
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if(localFiles[i].toString().contains(recResultPath)){
					while((line = in.readLine()) != null) {		
						String bookId = line.split("\\|", -1)[0];					
						recommendedBookSet.add(bookId);
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
