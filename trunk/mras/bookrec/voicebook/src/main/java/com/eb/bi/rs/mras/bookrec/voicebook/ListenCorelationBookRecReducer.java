package com.eb.bi.rs.mras.bookrec.voicebook;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras.bookrec.voicebook.util.StringIntPair;


public class ListenCorelationBookRecReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	private int listenCorelationRecNum;
	private double firstLowerBound;
	private double firstUpperBound;
	
	//听书基本信息
	private Map<String, String> bookBigClassMap = new HashMap<String, String>();
	
	// 听书精品库缓存数据
	// bigClassBookQualityMaps的数据格式是：bigClass|book1|...|bookn
	private Map<String, TreeSet<String>> bigClassBookQualityMap = new HashMap<String, TreeSet<String>>();
	
	//听书推荐库缓存数据
	//bigClassBookRecommendMap的数据格式是：bigClass|<book1, visit_cnt1>|...|<book2, visit_cnt2>
	private Map<String, TreeSet<StringIntPair>> bigClassBookRecommendMap = new HashMap<String, TreeSet<StringIntPair>>();
	
	//听书同系列图书
	//bookSeriesId的数据格式是：bookId|seriesId
	private Map<String, String> bookSeriesId = new HashMap<String, String>();
	
	//听书同系列图书
	//bookOrderId的数据格式是: bookId|orderId
	private Map<String, String> bookOrderId = new HashMap<String, String>();
	
			
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		
		/*************************************************************************
		 推书规则：
		1）	优先选择Class_type=1的图书；
		2）	若图书数不足，则选择class_type=2的图书；
		3）	若仍不足，则进行补白，补白规则为：精品库中，同大类下随机补白，若仍不足，则在听书推荐库表中，在同大类下按照月访问用户量降序补白
 		*************************************************************************/
		
		/*********************************************************************
		 *随机规则：
		 *chang修改后
		    1、得到第一本书的占比 10%-50% 
		 	   1）本身第一本书有百分比且满足范围限制，则用第一本书的本身百分比
		 	   2）若第一本书无百分比或有百分比但不满足10%-50%的范围，则随机一个10%-50%之间的值作为第一本书的百分比
			2、后续的占比获得方式  ：
				上限为：前一本书的百分比		
				下限为：前一本书的百分比*下线比 与 2.5%之间取较大值	
				下限比选择：
					若第一本书的百分比大于30%，则下线比为85%
					若第一本书的百分比大于20%，则下限比为90%
					若第一本书的百分比小于10%，则下线比为95%
		************************************************************************/
		TreeSet<String> cooccurrenceInfoSet = new TreeSet<String>(new Comparator<String>() {
			public int compare(String o1, String o2) {					
				String[] fields1 = o1.split("\\|",-1);
				String[] fields2 = o2.split("\\|",-1);					
				int ret = fields1[9].compareTo(fields2[9]);
				if(ret != 0) return ret;					
				double Confidence1 = Double.parseDouble(fields1[5]);
				double confidence2 = Double.parseDouble(fields2[5]);					
				if(confidence2 > Confidence1){
					return 1;
				}else if(confidence2 < Confidence1){
					return -1;
				}else {
					return fields1[1].compareTo(fields2[1]);
				}			
			}
		});	
		
		for(Text record : values){
			cooccurrenceInfoSet.add(record.toString());
			if(cooccurrenceInfoSet.size() > listenCorelationRecNum){
				cooccurrenceInfoSet.remove(cooccurrenceInfoSet.last());				
			}
		}

		
		Random rand = new Random();		
		HashSet<String> recBookSet = new HashSet<String>();
		String srcbookId = key.toString();
		
		StringBuffer result = new StringBuffer(srcbookId);
		int sequence = 0;
		
		//取得第一本书的信息，输入的共现信息里面已经排除了同系列的图书。
		Iterator<String> iter = cooccurrenceInfoSet.iterator();
		String[] fields = iter.next().split("\\|",-1);
		String bookId = fields[1];
		recBookSet.add(bookId);
		double confidence = Double.parseDouble(fields[5]);

		if( confidence > firstUpperBound || confidence < firstLowerBound){
			confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;			
		}
		result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
		++sequence;
		double subsequenceLowerBoundRate;
		if(confidence > 0.3) {
			subsequenceLowerBoundRate = 0.85;			
		}else if(confidence > 0.2) {
			subsequenceLowerBoundRate = 0.90;			
		}else {
			subsequenceLowerBoundRate = 0.95;
		}
		//选取共现图书
		while(iter.hasNext()) {
			bookId = iter.next().split("\\|",-1)[1];			
			recBookSet.add(bookId);	
			if(subsequenceLowerBoundRate * confidence > 0.025){
				confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
			}else {
				confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
			}			
			result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
			++sequence;
		}
		
		//听书精品库补白
		if (sequence < listenCorelationRecNum) {
			String[] bookQualityArray = null;
			if(bookBigClassMap.containsKey(srcbookId) && bigClassBookQualityMap.containsKey(bookBigClassMap.get(srcbookId)))
				bookQualityArray = bigClassBookQualityMap.get(bookBigClassMap.get(srcbookId)).toArray(new String[]{});
			int bookQualityLength = (bookQualityArray != null) ? bookQualityArray.length : 0;
			while(sequence < listenCorelationRecNum && bookQualityLength > 0) {
				int randomIndex = rand.nextInt(bookQualityLength);
				String qualityBookId = bookQualityArray[randomIndex];
				if (!recBookSet.contains(qualityBookId) && !qualityBookId.equals(srcbookId)) {
					recBookSet.add(qualityBookId);
					if(subsequenceLowerBoundRate * confidence > 0.025) {
						confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
					}
					else {
						confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
					}			
					result.append("|" + qualityBookId + "|" + String.format("%.1f%%", 100 * confidence));
					++sequence;			
				}
				String tmp = bookQualityArray[randomIndex];
				bookQualityArray[randomIndex] = bookQualityArray[--bookQualityLength];
				bookQualityArray[bookQualityLength] = tmp;
			}
		}
		
		//听书推荐库补白	
		if (sequence < listenCorelationRecNum) {
			StringIntPair[] bookRecommendArray = null;
			if(bookBigClassMap.containsKey(srcbookId) && bigClassBookRecommendMap.containsKey(bookBigClassMap.get(srcbookId)))
				bookRecommendArray = bigClassBookRecommendMap.get(bookBigClassMap.get(srcbookId)).toArray(new StringIntPair[]{});
			int bookRecommendLength = (bookRecommendArray != null) ? bookRecommendArray.length : 0;
			while(sequence < listenCorelationRecNum && bookRecommendLength > 0) {
				int randomIndex = rand.nextInt(bookRecommendLength);
				String recommendBookId = bookRecommendArray[randomIndex].getFirst();	
				if (!recBookSet.contains(recommendBookId) && !recommendBookId.equals(srcbookId)) {
					recBookSet.add(recommendBookId);
					if(subsequenceLowerBoundRate * confidence > 0.025) {
						confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
					}
					else {
						confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
					}			
					result.append("|" + recommendBookId + "|" + String.format("%.1f%%", 100 * confidence));
					++sequence;			
				}
				StringIntPair tmp = bookRecommendArray[randomIndex];
				bookRecommendArray[randomIndex] = bookRecommendArray[--bookRecommendLength];
				bookRecommendArray[bookRecommendLength] = tmp;
			}
		}	
		context.write(new Text(seriesBookFilter(result.toString())), NullWritable.get());
	}

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		listenCorelationRecNum = conf.getInt("listen.corelation.recommend.number", 10);		
		firstLowerBound = conf.getFloat("first.lower.bound", 0.1f);
		firstUpperBound = conf.getFloat("first.upper.bound", 0.5f);
		String bookListenQualityPath = conf.get("book.listen.quality.path");
		String bookListenRecommendPath = conf.get("book.listen.recommend.path");
		String bookInfoPath = conf.get("book.info.path");
		String bookSeriesPath = conf.get("book.series.path");

		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				if(localFiles[i].toString().contains(bookListenRecommendPath)){
					//听书推荐库表，数据格式为：bookId|visit_cnt|bigClassId|classId
					while((line = in.readLine()) != null) {
						String[] fields = line.split("\\|", -1);
						if(fields.length == 4){							
							String bookId = fields[0];
							String bigClass = fields[2];	
							StringIntPair pair = new StringIntPair(bookId, Integer.parseInt(fields[1]));	
							if(bigClassBookRecommendMap.containsKey(bigClass)) {
								TreeSet<StringIntPair> bookRecommendSet = bigClassBookRecommendMap.get(bigClass);
								bookRecommendSet.add(pair);
							} else {
								TreeSet<StringIntPair> bookRecommendSet = new TreeSet<StringIntPair>();
								bookRecommendSet.add(pair);
								bigClassBookRecommendMap.put(bigClass, bookRecommendSet);
							}
						}
					}
				}else if (localFiles[i].toString().contains(bookListenQualityPath)) {
					//听书精品库表，数据格式为：bookId|bigclassid|classid(6个月收听)
					while( (line = in.readLine()) != null) {			
						String fields[] = line.split("\\|",-1);
						if(fields.length == 3) {	
							String bookId = fields[0];
							String bigClass = fields[1];
							if(bigClassBookQualityMap.containsKey(bigClass)) {
								TreeSet<String> qualityBookSet = bigClassBookQualityMap.get(bigClass);
								qualityBookSet.add(bookId);
							} else {
								TreeSet<String> qualityBookSet = new TreeSet<String>();
								qualityBookSet.add(bookId);
								bigClassBookQualityMap.put(bigClass, qualityBookSet);
							}
						}					
					}					
				} else if(localFiles[i].toString().contains(bookInfoPath)) {
					//听书基本信息，数据格式为：bookId|bigClass|classId
					while((line = in.readLine()) != null) {
						String fields[] = line.split("\\|", -1);
						if(fields.length == 3) {
							bookBigClassMap.put(fields[0], fields[1]);
						}
					}
				} else if(localFiles[i].toString().contains(bookSeriesPath)) {
					//听书系列图书信息，数据格式为:bookId|seriesId|orderId
					while((line = in.readLine()) != null) {
						String fields[] = line.split("\\|", -1);
						if(fields.length == 3) {
							bookSeriesId.put(fields[0], fields[1]);
							bookOrderId.put(fields[0], fields[2]);
						}
					}
				}
			}finally {
				if(in != null){
					in.close();
				}
			}
		
		}
		
	}
	//过滤同系列图书，并根据orderId的不同，过滤掉不同情况的同系列图书
	//推荐结果中，同系列图书只保留一本，对于orderId以20**开头的，保留年份最大的，对于以1、2、3等情况，保留orderId最小的，对于odredId是999的情况，随机保留一本便可
	protected String seriesBookFilter(String str) {
		StringBuffer result = new StringBuffer();
		String fields[] = str.split("\\|", -1);
		String srcbookId = fields[0];
		result.append(srcbookId + ";");
		if(fields.length <= 1)
			return str;
		Map<String, String> beforeFilter = new HashMap<String, String>();
		Map<String, TreeSet<StringIntPair>> seriesBook = new HashMap<String, TreeSet<StringIntPair>>();
		Set<String> nonSeriesBook = new TreeSet<String>();
		for(int i = 1; i < fields.length-1; i += 2) {
			beforeFilter.put(fields[i], fields[i+1]);
		}
		Iterator<Entry<String, String>> iterator = beforeFilter.entrySet().iterator();
		Entry<String, String> item;
		String bookId;
		String seriesId;
		while(iterator.hasNext()) {
			item = iterator.next();
			bookId = item.getKey();
			if(bookSeriesId.containsKey(bookId)) {
				seriesId = bookSeriesId.get(bookId);
				if(seriesBook.containsKey(seriesId)) {
					TreeSet<StringIntPair> set = seriesBook.get(seriesId);
					if(bookOrderId.containsKey(bookId))
						set.add(new StringIntPair(bookId, Integer.parseInt(bookOrderId.get(bookId))));
				} else {
					TreeSet<StringIntPair> set = new TreeSet<StringIntPair>();
					if(bookOrderId.containsKey(bookId))
						set.add(new StringIntPair(bookId, Integer.parseInt(bookOrderId.get(bookId))));
					seriesBook.put(seriesId, set);
				}
			} else {
				nonSeriesBook.add(bookId);
			}
		}
		Iterator<Entry<String, TreeSet<StringIntPair>>> iter = seriesBook.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, TreeSet<StringIntPair>> entry = iter.next();
			String series = entry.getKey();
			TreeSet<StringIntPair> set = entry.getValue();
			if(set.size() > 0) {
				String book = null;
				String orderId = String.valueOf(set.first().getSecond());
				if(orderId.length() == 1) {
					book = set.first().getFirst();
				} else if(orderId.length() == 3) {
					book = set.first().getFirst();
				} else if(orderId.length() == 4) {
					book = set.last().getFirst();
				}
				result.append(book + "," + beforeFilter.get(book) + "|");
			}
		}
		Iterator<String> it = nonSeriesBook.iterator();
		while(it.hasNext()) {
			String book = it.next();
			result.append(book + "," + beforeFilter.get(book) + "|");
		}
		result.deleteCharAt(result.length()-1);
		return result.toString();
	}

}

