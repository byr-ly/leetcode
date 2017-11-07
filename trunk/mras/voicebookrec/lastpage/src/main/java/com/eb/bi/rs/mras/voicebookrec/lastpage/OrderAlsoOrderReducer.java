package com.eb.bi.rs.mras.voicebookrec.lastpage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;



import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;



/*
 * 选取非同系列的同栏目图书，按照共同订购用户数或者rate降序排列，
 * 向上补足10本推荐，若不足10本则同栏目热书补白，补足10本（需要确保订购还订购最终的10本图书中不包含源图书的同系列图书）
 *	增加一个逻辑：
 *		订购还订购的补白书中，从同栏目热书中去掉同系列热书。
 * 
 */
public class OrderAlsoOrderReducer extends Reducer<Text, StringDoublePair, Text, NullWritable> {

	//存储栏目热书信息
	HashMap<String, TreeSet<StringIntPair>> columnHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	//存储全局热书信息
	TreeSet<StringIntPair> hotBookSet = new TreeSet<StringIntPair>();
	//存储图书信息
	HashMap<String, BookInfo> bookInfoMap = new HashMap<String, BookInfo>();
	//存储订购频次小于订购频次阈值的听书ID，不再为该类听书推荐共现矩阵中的图书
	HashSet<String> orderFrequeyLessSet = new HashSet<String>();
	//订购图书书目
	int orderRecNumber = 10;
	//调整rate时，开几次方
	int orderPowNumber = 2;
	//听书订购频次的阈值
	int orderFrequenyThreshold = 5;	
	//rate调整系数。
	float orderRationAdjustFactor = 1.0f;
	
	protected void reduce(Text key, Iterable<StringDoublePair> values, Context context) throws IOException ,InterruptedException {
		TreeSet<StringDoublePair> cooccurrenceInfoSet = new TreeSet<StringDoublePair>();

		for(StringDoublePair pair : values){
			if("0".equals(pair.getFirst())){
				break;
			}
			if("1".equals(pair.getFirst())){
				continue;
			}
			cooccurrenceInfoSet.add(new StringDoublePair(pair));
			if(cooccurrenceInfoSet.size() > orderRecNumber){
				cooccurrenceInfoSet.remove(cooccurrenceInfoSet.last());
			}
		}
		/*日期|源图书ID|推荐图书1|占比1|推荐图书2|占比2|……推荐图书10|占比10*/
		String srcBookId = key.toString();
		
		Date date = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMdd");
		String line = format.format(date) + "|" + srcBookId + "|";
		
		HashSet<String> cooccurrenceBookIdSet = new HashSet<String>();
		if(!orderFrequeyLessSet.contains(srcBookId)) {
			Iterator<StringDoublePair> iter = cooccurrenceInfoSet.iterator();
			while(iter.hasNext()){
				StringDoublePair pair = iter.next();
				String bookId = pair.getFirst();			
				line += bookId + "|" + orderRationAdjustFactor * Math.pow(pair.getSecond(), 1.0/orderPowNumber) + "|";		/*调整rate*/			
				cooccurrenceBookIdSet.add(bookId);
			}			
		}
		
		int count = cooccurrenceBookIdSet.size();		
		TreeSet<StringIntPair> hotBookSet = null;
		String columnId = bookInfoMap.get(srcBookId).getColumnId();
		if (columnHotBookMap.containsKey(columnId)) {
			hotBookSet = columnHotBookMap.get(columnId);
		} else {
			hotBookSet = this.hotBookSet;
		}
		
		Iterator<StringIntPair> hotBookIter = hotBookSet.iterator();
		while(hotBookIter.hasNext()){
			if(count >= orderRecNumber ) break;			
			StringIntPair item = hotBookIter.next();
			String bookId = item.getFirst();
			if(!cooccurrenceBookIdSet.contains(bookId) && !bookId.equals(srcBookId) && bookInfoMap.containsKey(bookId)) {//判断是否出现在共现推荐结果中，以及是否与源图书相同,以及保证其在线
				line += bookId + "||";
				++count;
			}		
		}
		while(count < orderRecNumber){
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

		orderRecNumber = conf.getInt("order.recommend.number",10);
		orderPowNumber = conf.getInt("order.pow.number", 2);
		orderFrequenyThreshold = conf.getInt("order.frequency.threshold", 5);
		orderRationAdjustFactor = conf.getFloat("order.ratio.adjust.factor", 1.0f);

		String bookInfoPath = conf.get("book.info.path");
		String orderColumnbookFrequencyPath = conf.get("order.column.book.frequency.path");
		
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				if(localFiles[i].toString().contains(orderColumnbookFrequencyPath)){
					while((line = in.readLine()) != null) {/*图书ID|栏目Id|订购次数*/						
						String[] fields = line.split("\\|", -1);
						if(fields.length == 3){
							String bookId = fields[0];
							String columnId = fields[1];
							int frequency = Integer.parseInt(fields[2]);							
							if(frequency <= orderFrequenyThreshold){
								orderFrequeyLessSet.add(bookId);								
							}
							StringIntPair pair = new StringIntPair(bookId, frequency);
							hotBookSet.add(pair);
							if (hotBookSet.size() > orderRecNumber + 1 ) {
								hotBookSet.remove(hotBookSet.last());								
							}
							if(columnHotBookMap.containsKey(columnId)){
								TreeSet<StringIntPair> hotBookset = columnHotBookMap.get(columnId);							
								hotBookset.add(pair);
								if(hotBookset.size() > orderRecNumber + 1){
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
			}finally {
				if(in != null){
					in.close();
				}
			}
		
		}
	}
}
