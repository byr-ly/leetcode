package com.eb.bi.rs.mras.bookrec.corelationrec.CommonUtil;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadCorelationBookRecReducer extends Reducer<TextPair, TextPair, Text, NullWritable>{
	
	private int readCorelationRecNum;

	private double firstLowerBound ;
	private double firstUpperBound ;


	private HashMap<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private HashMap<String, TreeSet<StringIntPair>> authorHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private HashMap<String, TreeSet<StringIntPair>> bigclassHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private HashMap<String, String>  bookClassMap = new HashMap<String, String>();
	private HashMap<String, String>  bookAuthorMap = new HashMap<String, String>();
	private HashMap<String, String>  bookBigClassMap = new HashMap<String, String>();
	private Map<String, String> bookSeriesMap = new HashMap<String, String>();
	private Map<String, Set<String>> seriesBookSetMap = new HashMap<String, Set<String>>();	
	private Set<String> freeBookSet = new HashSet<String>();
			
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {
		
		/*************************************************************************
		 推书规则：
		1）	优先选择Class_type=1的图书；
		2）	若图书数不足，则选择class_type=2的图书；
		3）	若仍不足，则进行补白，补白规则为：同大类下按照按照图书本身热度降序补足
		*************************************************************************/
		
		/*********************************************************************
		 *随机规则：
		 *chang 修改
		    1、得到第一本书的占比 10%-35% 
		 	   1）本身第一本书有百分比且满足范围限制，则用第一本书的本身百分比
		 	   2）若第一本书无百分比或有百分比但不满足10%-35%的范围，则随机一个10%-35%之间的值作为第一本书的百分比
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

		for (TextPair pair : values) {				
			cooccurrenceInfoSet.add(pair.getFirst().toString());
			if(cooccurrenceInfoSet.size() > 2 * readCorelationRecNum + 1 ){
				cooccurrenceInfoSet.remove(cooccurrenceInfoSet.last());				
			}				
		}			
		
		if (cooccurrenceInfoSet.size() == 0) {
			return;
		}
		
		String srcbookId = key.getFirst().toString();		
		HashSet<String> recBookSet = new HashSet<String>();
		StringBuffer result = new StringBuffer(srcbookId);
		int sequence = 0;
	
		
		Random rand = new Random();		
		double confidence = 0.0;
		Iterator<String> iter = cooccurrenceInfoSet.iterator();
		//找到第一本书，输入的共现信息里面已经排除了同系列的图书。
		while(iter.hasNext()){
			String[] fields = iter.next().split("\\|",-1);
			String bookId = fields[1];
			//chang 关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
			if(!bookId.equals(srcbookId)) {
				recBookSet.add(bookId);
				confidence = Double.parseDouble(fields[5]);
				if( confidence > firstUpperBound || confidence < firstLowerBound){
					confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;					
				}
				result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
				++sequence;
				break;
			}
		}
		if(sequence == 0) {//如果全是补白的数据，需要生成first confidence数据
			confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;	
		}
		double subsequenceLowerBoundRate;
		if (confidence > 0.3) {
			subsequenceLowerBoundRate = 0.85;			
		} else if(confidence > 0.2) {
			subsequenceLowerBoundRate = 0.90;			
		} else {
			subsequenceLowerBoundRate = 0.95;
		}		
		//选取共现图书
		while(iter.hasNext()){
			if(sequence >= readCorelationRecNum) break;
			String[] fields = iter.next().split("\\|",-1);
			String bookId = fields[1];
			//chang 关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
			if(!bookId.equals(srcbookId)) {
				recBookSet.add(bookId);	
				if(subsequenceLowerBoundRate * confidence > 0.025){
					confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
				}
				else {
					confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
				}
				result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
				++sequence;
			}
		}
		Set<String> seriesBookSet = null;
		if (bookSeriesMap.containsKey(srcbookId)) {						
			 seriesBookSet = seriesBookSetMap.get(bookSeriesMap.get(srcbookId));
		}
		Set<String> picked = new HashSet<String>();
		//测试
		//System.out.println();
		//System.out.println("元图书："+srcbookId);
		//System.out.println(srcbookId+"；"+authorHotBookMap.get(bookAuthorMap.get(srcbookId)).toString());
		//同作者补白	
		if (sequence < readCorelationRecNum&&bookAuthorMap.containsKey(srcbookId)) {
			StringIntPair[] BookArray = authorHotBookMap.get(bookAuthorMap.get(srcbookId)).toArray(new StringIntPair[]{});
			
			for(int i=0;i<BookArray.length;i++){
				String BookId = BookArray[i].getFirst();	
				//关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
				picked.add(BookId);
				if( !recBookSet.contains(BookId) && (seriesBookSet == null || !seriesBookSet.contains(BookId)) && !BookId.equals(srcbookId) && !freeBookSet.contains(BookId) ) {
					recBookSet.add(BookId);
					if(subsequenceLowerBoundRate * confidence > 0.025){
						confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
					}
					else {
						confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
					}			
					result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));
					++sequence;	
					//测试
					//System.out.print("作者补白:"+sequence+"-"+BookId+"|");
					
				}
				if(sequence >= readCorelationRecNum){
					break;
				}
			} 		
		}
		//测试
		//System.out.println();
		//System.out.println(srcbookId+"；"+classHotBookMap.get(bookClassMap.get(srcbookId)).toString());
		//分类热书补白	
		if (sequence < readCorelationRecNum) {
			StringIntPair[] hotBookArray = classHotBookMap.get(bookClassMap.get(srcbookId)).toArray(new StringIntPair[]{});
			Set<Integer> used = new HashSet<Integer>();
			do {				
				int idx = rand.nextInt(hotBookArray.length);
				used.add(idx);
				while(picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
					idx = rand.nextInt(hotBookArray.length);
					used.add(idx);
				}
				if(!picked.contains(hotBookArray[idx].getFirst())){
					String hotBookId = hotBookArray[idx].getFirst();	
					picked.add(hotBookId);
					//chang 关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
					if( !recBookSet.contains(hotBookId) && (seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId) ) {
						recBookSet.add(hotBookId);
						if(subsequenceLowerBoundRate * confidence > 0.025){
							confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
						}
						else {
							confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
						}			
						result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
						++sequence;	
						//测试
						//System.out.print("分类补白"+sequence+"-"+ hotBookId+"|");
					}
				}				
			} while (sequence < readCorelationRecNum && used.size()<hotBookArray.length);			
		}
		//测试
		//System.out.println();
		//System.out.println(srcbookId+"；"+bigclassHotBookMap.get(bookClassMap.get(srcbookId)).toString());
		//大类热书补白
		if (sequence < readCorelationRecNum) {
			StringIntPair[] hotBookArray = bigclassHotBookMap.get(bookBigClassMap.get(srcbookId)).toArray(new StringIntPair[]{});
			Set<Integer> used = new HashSet<Integer>();
			do {				
				int idx = rand.nextInt(hotBookArray.length);
				used.add(idx);
				while(picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
					idx = rand.nextInt(hotBookArray.length);
					used.add(idx);
				}
				if(!picked.contains(hotBookArray[idx].getFirst())){
					String hotBookId = hotBookArray[idx].getFirst();	
					picked.add(hotBookId);
					//chang 关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
					if( !recBookSet.contains(hotBookId) && (seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId) ) {
						recBookSet.add(hotBookId);
						if(subsequenceLowerBoundRate * confidence > 0.025){
							confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
						}
						else {
							confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
						}			
						result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
						++sequence;	
						//测试
						//System.out.print("大类补白"+sequence+"-"+ hotBookId+"|");
					}			
				}	
			} while (sequence < readCorelationRecNum && used.size()<hotBookArray.length);			
		}
		
		//System.out.println("补白最终数量"+sequence);
		context.write(new Text(result.toString()), NullWritable.get());
		
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		readCorelationRecNum = conf.getInt("read.corelation.recommend.number", 10);	
		firstLowerBound = conf.getFloat("first.lower.bound", 0.1f) ;
		firstUpperBound = conf.getFloat("first.upper.bound", 0.35f) ;		
		String bookClassFrequencyPath = conf.get("book.class.frequency.path");		

		String bookSeriesPath = conf.get("book.series.path");
		int hotBookCount = conf.getInt("select.hotbook.count.for.random", 200);
		//add
		String freeBookPath = conf.get("free.book.path");
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for(int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(localFiles[i].toString()));
				if(localFiles[i].toString().contains(bookClassFrequencyPath)) {
					while((line = in.readLine()) != null) {/*图书ID|作者|分类|大类|频次*/
						String[] fields = line.split("\\|", -1);
						if(fields.length == 5) {
							String bookId = fields[0];
							String author = fields[1];
							String classType = fields[2];
							String bigClass = fields[3];
							if(!author.equals("")){
								bookAuthorMap.put(bookId, author);
							}
							bookClassMap.put(bookId, classType);
							bookBigClassMap.put(bookId, bigClass);
							StringIntPair pair = new StringIntPair(bookId,Integer.parseInt(fields[4]));	
							
							if(!author.equals("")){
								if (authorHotBookMap.containsKey(author)) {
									TreeSet<StringIntPair> Bookset = authorHotBookMap.get(author);	
										Bookset.add(pair);
								}else {
									TreeSet<StringIntPair> Bookset = new TreeSet<StringIntPair>();
									Bookset.add(pair);
									authorHotBookMap.put(author, Bookset);							
								}
							}							
							if (classHotBookMap.containsKey(classType)) {
								TreeSet<StringIntPair> hotBookset = classHotBookMap.get(classType);	
								if (hotBookset.size() < hotBookCount) {
									hotBookset.add(pair);
								} else if (pair.compareTo(hotBookset.last()) < 0) {									
									hotBookset.remove(hotBookset.last());
									hotBookset.add(pair);
								}
							}else {
								TreeSet<StringIntPair> hotBookset = new TreeSet<StringIntPair>();
								hotBookset.add(pair);
								classHotBookMap.put(classType, hotBookset);							
							}
							if (bigclassHotBookMap.containsKey(bigClass)) {
								TreeSet<StringIntPair> hotbigclassBookset = bigclassHotBookMap.get(bigClass);	
								if (hotbigclassBookset.size() < hotBookCount) {
									hotbigclassBookset.add(pair);
								} else if (pair.compareTo(hotbigclassBookset.last()) < 0) {									
									hotbigclassBookset.remove(hotbigclassBookset.last());
									hotbigclassBookset.add(pair);
								}
							}else {
								TreeSet<StringIntPair> hotbigclassBookset = new TreeSet<StringIntPair>();
								hotbigclassBookset.add(pair);
								bigclassHotBookMap.put(bigClass, hotbigclassBookset);							
							}
						}
					}
				} else if (localFiles[i].toString().contains(bookSeriesPath)) {
					while( (line = in.readLine()) != null){			
						String fields[] = line.split("\\|",-1);
						String bookId = fields[0];
						String seriesId = fields[1];
						if(fields.length == 2) {//bookid|series_id							
							bookSeriesMap.put(bookId, seriesId);
							if (seriesBookSetMap.containsKey(seriesId)) {
								Set<String> bookSet = seriesBookSetMap.get(seriesId);
								bookSet.add(bookId);
							} else {
								Set<String> bookSet = new HashSet<String>();
								bookSet.add(bookId);
								seriesBookSetMap.put(seriesId, bookSet);								
							}									
						}					
					}					
				}else if (localFiles[i].toString().contains(freeBookPath)) {
					while( (line = in.readLine()) != null) {
						freeBookSet.add(line.trim());
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
