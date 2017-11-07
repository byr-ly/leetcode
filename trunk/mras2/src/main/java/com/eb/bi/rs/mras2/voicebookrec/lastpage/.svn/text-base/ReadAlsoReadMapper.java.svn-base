package com.eb.bi.rs.mras2.voicebookrec.lastpage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;


/* 业务需求：
	根据共同阅读（听书）表（已去异常数据），选取非同系列的同栏目图书，按照共同阅读用户数，或者rate降序排列，向上补足10本推荐，若不足10本则同栏目热书补白，补足10本；
 */
public class ReadAlsoReadMapper extends Mapper<LongWritable, Text, Text, StringDoublePair> {

	//存储图书信息
	HashMap<String, BookInfo> bookInfoMap = new HashMap<String, BookInfo>();
	//存储共现信息
	HashMap<String, TreeSet<StringDoublePair>>  cooccurrenceInfoMap = new HashMap<String, TreeSet<StringDoublePair>>();
	//阅读还阅读推荐听书数目
	int readRecNumber = 10;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String[] fields = value.toString().split("\\|",-1);
		String srcBookId = fields[0];
		BookInfo srcBookInfo = bookInfoMap.get(srcBookId);
		if(srcBookInfo != null && !"".equals(srcBookInfo.getColumnId())){//不处理没有听书信息或者存在听书信息但是其栏目id为空的源图书。
			if(fields.length == 3) {				
				String dstBookId = fields[1];	
				double rate = Double.parseDouble(fields[2]);				
				BookInfo dstBookInfo = bookInfoMap.get(dstBookId);
				if(dstBookInfo != null) {
					String srcSerialId = srcBookInfo.getSerialId();
					String dstSerialId = dstBookInfo.getSerialId();
					//同栏目非同系列
					if( (srcBookInfo.getColumnId().equals(dstBookInfo.getColumnId())) && ("".equals(srcSerialId) || "-1".equals(srcSerialId) || "".equals(dstSerialId) || "-1".equals(dstSerialId) ||!srcSerialId.equals(dstSerialId))) {							
						if(cooccurrenceInfoMap.containsKey(srcBookId)){
							TreeSet<StringDoublePair> treeSet = cooccurrenceInfoMap.get(srcBookId);
							treeSet.add(new StringDoublePair(dstBookId, rate));
							if(treeSet.size() > readRecNumber){
								treeSet.remove(treeSet.last());
							}					
						}else {
							TreeSet<StringDoublePair> treeSet = new TreeSet<StringDoublePair>();
							treeSet.add(new StringDoublePair(dstBookId,rate));
							cooccurrenceInfoMap.put(srcBookId, treeSet);			
						}					
					}else {//如果共现图书都非“同栏目非同系列”，还是要为该听书推荐听书。
						context.write(new Text(srcBookId), new StringDoublePair("1",0));
					}					
				}else {//如果共现图书都下架，还是要为该听书推荐听书的				
					context.write(new Text(srcBookId), new StringDoublePair("1",0));					
				}
			}else if(fields.length == 1){
				context.write(new Text(srcBookId), new StringDoublePair("0",0));//为没有出现在共现矩阵中的听书推荐听书
			}
		}//不处理没有听书信息或者存在听书信息但是其栏目id为空的源图书
	}
	
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String bookInfoPath = conf.get("book.info.path");
		readRecNumber = conf.getInt("read.recommend.number",10);

//		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        URI[] localFiles = context.getCacheFiles();

        for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;

            if(localFiles[i].toString().contains(bookInfoPath)){
				try {
		        	FileSystem fs = FileSystem.get(localFiles[i], conf);
		        	in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
                    while ((line = in.readLine()) != null) {
                        BookInfo bookInfo = new BookInfo(line);
                        bookInfoMap.put(bookInfo.getBookId(), bookInfo);
                    }
                }finally {
					if(in != null){
						in.close();
					}
				}

			}
		}
	}
	protected void cleanup(Context context) throws IOException ,InterruptedException {		
		Iterator<Entry<String, TreeSet<StringDoublePair>>> iter = cooccurrenceInfoMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, TreeSet<StringDoublePair>> item = iter.next();
			Iterator<StringDoublePair> setIter = item.getValue().iterator();
			while(setIter.hasNext()){
				context.write(new Text(item.getKey()), setIter.next());
			}			
		}		
	}
}
