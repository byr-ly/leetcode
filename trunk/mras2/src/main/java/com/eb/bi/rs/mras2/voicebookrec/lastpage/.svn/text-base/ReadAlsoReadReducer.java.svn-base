package com.eb.bi.rs.mras2.voicebookrec.lastpage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras2.voicebookrec.lastpage.BookInfo;
import com.eb.bi.rs.mras2.voicebookrec.lastpage.StringDoublePair;
import com.eb.bi.rs.mras2.voicebookrec.lastpage.StringIntPair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/*
 * 选取非同系列的同栏目图书，按照共同订购用户数或者rate降序排列，
 * 向上补足10本推荐，若不足10本则同栏目热书补白，补足10本
 * 根据共同订购用户数降序，取前10本进行听书推荐，其中需要排除源图书同序列的其他图书。
 */
public class ReadAlsoReadReducer extends Reducer<Text, StringDoublePair, Text, NullWritable> {

	/*存储同栏目热书信息*/
	HashMap<String, TreeSet<StringIntPair>> columnHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	//存储全局热书信息
	TreeSet<StringIntPair> hotBookSet = new TreeSet<StringIntPair>();
	/*存储图书信息*/
	HashMap<String, BookInfo> bookInfoMap = new HashMap<String, BookInfo>();
	/*存储已推荐序列图书*/
	HashMap<String, HashSet<String>> serialBookMap = new HashMap<String, HashSet<String>>();
	/*存储订购还订购图书*/
	HashMap<String, HashSet<String>> orderAlsoOrderMap = new HashMap<String, HashSet<String>>();
	//存储阅读频次小于阅读频次阈值的听书ID，不再为该类听书推荐共现矩阵中的图书
	HashSet<String> readFrequeyLessSet = new HashSet<String>();
	/*阅读还阅读推荐图书数据*/
	int readRecNumber = 10;
	/*rate调整时，开几次方*/
	int powNumber = 2;
	/*听书阅读频次的阈值*/
	int readFrequenyThreshold = 10;
	//rate调整系数。
	float readRationAdjustFactor = 1.0f;

	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws IOException ,InterruptedException {
		double upperBound = 0.35;
		double lowerBound = 0.025;
		double confidence = 0;
		
		TreeSet<StringDoublePair> cooccurrenceInfoSet = new TreeSet<StringDoublePair>();
		for(StringDoublePair pair : values){
			if("0".equals(pair.getFirst())){
				break;
			}
			if("1".equals(pair.getFirst())){
				continue;
			}
			cooccurrenceInfoSet.add(new StringDoublePair(pair));
			if(cooccurrenceInfoSet.size() > 2 * readRecNumber + 1) {
				cooccurrenceInfoSet.remove(cooccurrenceInfoSet.last());
			}
		}
		/*日期|源图书ID|推荐图书1|占比1|推荐图书2|占比2|……推荐图书10|占比10*/
		String srcBookId = key.toString();
		
		Date date = new Date();
		//SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMdd");
		String line = srcBookId + ";";
		
		/*记录已推荐图书*/
		HashSet<String> resultBookIdSet = new HashSet<String>();		
		
		/*订购还订购结果，阅读还阅读推荐结果需要过滤掉订购还订购的结果*/
		HashSet<String> orderBookSet = orderAlsoOrderMap.get(srcBookId);
		
		Random rand = new Random();	
		/*推荐同系列图书*/
		if(serialBookMap.containsKey(srcBookId)){
			HashSet<String> seriaBooklSet = serialBookMap.get(srcBookId);
			Iterator<String> iter= seriaBooklSet.iterator();
			while(iter.hasNext()){
				String serialBookid = iter.next();
				if(bookInfoMap.containsKey(serialBookid)) {//保证推荐图书未下架
					confidence = rand.nextDouble() * (upperBound - lowerBound) + lowerBound;
					line += serialBookid + "," + String.format("%.1f%%", 100 * confidence) + "|";
					upperBound = confidence;
					resultBookIdSet.add(serialBookid);
				}
			}			
		}		
		
		/*推荐同栏目非同系列共现图书*/
		if(!readFrequeyLessSet.contains(srcBookId)){			
			Iterator<StringDoublePair> iter = cooccurrenceInfoSet.iterator();
			while(iter.hasNext()){
				if(resultBookIdSet.size() >= readRecNumber) break;
				StringDoublePair pair = iter.next();			
				String cooccurrenceBookId = pair.getFirst();
				if(!orderBookSet.contains(cooccurrenceBookId) ) {
					confidence = rand.nextDouble() * (upperBound - lowerBound) + lowerBound;
					line += cooccurrenceBookId + "," + String.format("%.1f%%", 100 * confidence) + "|";
					upperBound = confidence;
					//line += cooccurrenceBookId + "|" + readRationAdjustFactor * Math.pow(pair.getSecond(), 1.0/powNumber) + "|";		/*调整rate*/				
					resultBookIdSet.add(cooccurrenceBookId);
				}		
			}			
		}

		TreeSet<StringIntPair> hotBookSet = null;
		String column = bookInfoMap.get(srcBookId).getColumnId();
		if (columnHotBookMap.containsKey(column)) {
			hotBookSet = columnHotBookMap.get(column);
		} else {
			hotBookSet = this.hotBookSet;
		}

		Iterator<StringIntPair> hotBookIter = hotBookSet.iterator();		
		
		while(hotBookIter.hasNext()){
			if(resultBookIdSet.size() >= readRecNumber ) break;			
			StringIntPair item = hotBookIter.next();
			String hotbookId = item.getFirst();
			if(!srcBookId.equals(hotbookId) && !resultBookIdSet.contains(hotbookId) && !orderBookSet.contains(hotbookId) && bookInfoMap.containsKey(hotbookId)) {//保证推荐图书未下架 
				confidence = rand.nextDouble() * (upperBound - lowerBound) + lowerBound;
				line += hotbookId + "," +String.format("%.1f%%", 100 * confidence) + "|";
				upperBound = confidence;
				//line += hotbookId + "||";
				resultBookIdSet.add(hotbookId);
			}		
		}
		int count = resultBookIdSet.size();
		while(count < readRecNumber){
			line += "||";
			++count;
		}
		if(line.endsWith("|")){
			line = line.substring(0, line.length() - 1);
		}
		context.write(new Text(line), NullWritable.get());		
	}
	

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);		
		Configuration conf = context.getConfiguration();

		readRecNumber = conf.getInt("read.recommend.number",10);
		powNumber = conf.getInt("read.pow.number", 2);
		readFrequenyThreshold = conf.getInt("read.frequency.threshold", 10);
		readRationAdjustFactor = conf.getFloat("read.ratio.adjust.factor", 1.0f);
		
		String bookInfoPath = conf.get("book.info.path");
		String readColumnbookFrequencyPath = conf.get("read.column.book.frequency.path");
		String readSerialBookPath = conf.get("read.serial.book.path");
		String orderAlsoOrderPath = conf.get("order.also.order.path");

		
		//Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        URI[] localFiles = context.getCacheFiles();

		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
            	FileSystem fs = FileSystem.get(localFiles[i], conf);
            	in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if(localFiles[i].toString().contains(readColumnbookFrequencyPath)){
					while((line = in.readLine()) != null) {/*图书ID|栏目Id|订购次数*/						
						String[] fields = line.split("\\|", -1);
						if(fields.length == 3) {
							String bookId = fields[0];
							String columnId = fields[1];
							int frequency = Integer.parseInt(fields[2]);
							if(frequency <= readFrequenyThreshold){
								readFrequeyLessSet.add(bookId);								
							}
							StringIntPair  pair = new StringIntPair(bookId,frequency);
							hotBookSet.add(pair);
							if (hotBookSet.size() > 2 * readRecNumber + 1 ) {
								hotBookSet.remove(hotBookSet.last());
							}							
							if(columnHotBookMap.containsKey(columnId)){
								TreeSet<StringIntPair> hotBookset = columnHotBookMap.get(columnId);							
								hotBookset.add(pair);
								if(hotBookset.size() > 2 * readRecNumber + 1){//因为要去掉订购还订购的结果，所以要多放些数据
									hotBookset.remove(hotBookset.last());//不要写成first了
								}							
							}else {
								TreeSet<StringIntPair> hotBookset = new TreeSet<StringIntPair>();
								hotBookset.add(pair);
	 							columnHotBookMap.put(columnId, hotBookset);							
							}											
						}
					}					
				}				
				else if(localFiles[i].toString().contains(bookInfoPath)){
					while((line = in.readLine()) != null) {						
						BookInfo bookInfo = new BookInfo(line);					
						bookInfoMap.put(bookInfo.getBookId(), bookInfo);							
					}
				}
				else if(localFiles[i].toString().contains(readSerialBookPath)){
					while((line = in.readLine()) != null) {
						String[] fields = line.split("\\|", -1);
						if(fields.length == 2){
							String book1 = fields[0];
							String book2 = fields[1];
							if(serialBookMap.containsKey(book1)){
								serialBookMap.get(book1).add(book2);								
							}else {
								HashSet<String> set = new HashSet<String>();
								set.add(book2);
								serialBookMap.put(book1, set);
							}						
						}						
					}
				}
				else if(localFiles[i].toString().contains(orderAlsoOrderPath)){/*######*/
					while((line = in.readLine()) != null) {
						/*日期|源图书ID|推荐图书1|占比1|推荐图书2|占比2|……推荐图书10|占比10*/
						String[] fields = line.split("\\|", -1);					
						String book1 = fields[1];
						
						HashSet<String> set = new HashSet<String>();
						for(int j = 2; j < fields.length ; j= j+2){
							set.add(fields[j]);
						}
						orderAlsoOrderMap.put(book1, set);					
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
