package com.eb.bi.rs.mras.bookrec.corelationrec.CommonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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

public class ReadRecForBookWithoutCooccurrenceInfoReducer extends Reducer<TextPair, TextPair, Text, NullWritable> {

	private int corelationRecNum;
	private Map<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private Map<String, TreeSet<StringIntPair>> authorHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private Map<String, TreeSet<StringIntPair>> bigclassHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private Map<String, String> bookSeriesMap = new HashMap<String, String>();
	private Map<String, Set<String>> seriesBookSetMap = new HashMap<String, Set<String>>();
	private Set<String> freeBookSet = new HashSet<String>();
	
	//关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 
	private Map<String, Long> bookClickMap = new HashMap<String, Long>(); 
	private double firstLowerBound;
	private double firstUpperBound;

	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
	        String [] ff = key.getFirst().toString().split("\\|",-1);
	        String srcbookId = ff[0];
	        String author =ff[1]; 
	        String classType = ff[2];
	        String bigClass = ff[3];
			
			StringBuffer result = new StringBuffer(srcbookId);
			int sequence = 0;
			Random rand = new Random();
			double confidence = rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;
			double subsequenceLowerBoundRate;
			if(confidence > 0.3) {
				subsequenceLowerBoundRate = 0.85;			
			}else if(confidence > 0.2) {
				subsequenceLowerBoundRate = 0.90;			
			}else {
				subsequenceLowerBoundRate = 0.95;
			}
			Set<String> seriesBookSet = null;
			if (bookSeriesMap.containsKey(srcbookId)) {
				seriesBookSet = seriesBookSetMap.get(bookSeriesMap.get(srcbookId));
			}
			Set<String> picked = new HashSet<String>();
			//测试
			//System.out.println();
			//System.out.println("元图书："+srcbookId);
			//System.out.println(srcbookId+"；"+authorHotBookMap.get(author).toString());
			//作家补白
			if (!author.equals("")&&authorHotBookMap.containsKey(author)) {
				StringIntPair[] BookArray = authorHotBookMap.get(author).toArray(new StringIntPair[] {});
				int i=0;
				if(bookClickMap.containsKey(srcbookId) && bookClickMap.get(srcbookId) > 500) {
					if(sequence < corelationRecNum) {
						while(i<BookArray.length){
							String BookId = BookArray[i].getFirst();
							picked.add(BookId);
							if((seriesBookSet == null || !seriesBookSet.contains(BookId)) && !BookId.equals(srcbookId) && !freeBookSet.contains(BookId)) {
								if(sequence == 0) {
									result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));
								} else {
									if(subsequenceLowerBoundRate * confidence > 0.025){
										confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
									}else {
										confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
									}			
									result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));						
								}
								++sequence;	
								//测试
								//System.out.print("作者补白"+BookId+"|");
							}
							++i;
							if(sequence >= corelationRecNum){
								break;
							}
						}
					}
				}else{
					if(sequence < corelationRecNum) {
						while(i<BookArray.length){
							String BookId = BookArray[i].getFirst();
							picked.add(BookId);
							if((seriesBookSet == null || !seriesBookSet.contains(BookId)) && !BookId.equals(srcbookId) && !freeBookSet.contains(BookId)) {
								result.append("|" + BookId + "|");
								++sequence;		
								//测试
								//System.out.print("作者补白"+BookId+"|");
							}
							++i;
							if(sequence >= corelationRecNum){
								break;
							}
						}			
					}
				}
			}
			//测试
			//System.out.println("分类补白开始");
			//System.out.println(srcbookId+"；"+classHotBookMap.get(classType).toString());
			//分类热书补白
			if (classHotBookMap.containsKey(classType)) {				
				StringIntPair[] hotBookArray = classHotBookMap.get(classType).toArray(new StringIntPair[] {});
				Set<Integer> used = new HashSet<Integer>();
				//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 ，如果源图书的点击量大于500，那么就给补全的图书添加上随机百分比，否则就不添加
				if(bookClickMap.containsKey(srcbookId) && bookClickMap.get(srcbookId) > 500) {
					//由于是随机补充百分比，所以我们可以先在while循环外面计算出第一本书的百分比，然后在遇到sequence == 0的条件时，就可以把这本书的百分比添加进去
					
				   while(sequence < corelationRecNum && used.size()<hotBookArray.length) {
						int idx = rand.nextInt(hotBookArray.length);
						used.add(idx);
						//测试
						//System.out.println("分类补白idx"+ idx);
						while(picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
							idx = rand.nextInt(hotBookArray.length);
							used.add(idx);
							//测试
							//System.out.println("分类补白idx"+ idx);
						}
						if(!picked.contains(hotBookArray[idx].getFirst())){
							String hotBookId = hotBookArray[idx].getFirst();
							picked.add(hotBookId);
							if((seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId)) {
								if(sequence == 0) {
									result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
								} else {
									if(subsequenceLowerBoundRate * confidence > 0.025){
										confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
									}else {
										confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
									}			
									result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));						
								}
								++sequence;	
								//测试
								//System.out.print("分类补白"+ hotBookId+"|");
							}
						}	
					}
				} else {
					
					while (sequence < corelationRecNum && used.size()<hotBookArray.length) {				
						int idx = rand.nextInt(hotBookArray.length);
						used.add(idx);
						//测试
						//System.out.println("分类补白idx"+ idx);
						while (picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
							idx = rand.nextInt(hotBookArray.length);
							used.add(idx);
							//测试
							//System.out.println("分类补白idx"+ idx);
						}
						if(!picked.contains(hotBookArray[idx].getFirst())){
							String hotBookId = hotBookArray[idx].getFirst();
							picked.add(hotBookId);
							if((seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId)) {
								result.append("|" + hotBookId + "|");
								++sequence;	
								//测试
								//System.out.print("分类补白"+ hotBookId+"|");
							}
						}				
					}
				}
				
			}
			//测试
			//System.out.println("大类补白开始");
			//System.out.println(srcbookId+"；"+bigclassHotBookMap.get(bigClass).toString());
			//大类热书补白
			if (bigclassHotBookMap.containsKey(bigClass)) {				
				StringIntPair[] hotBookArray = bigclassHotBookMap.get(bigClass).toArray(new StringIntPair[] {});
				Set<Integer> used = new HashSet<Integer>();
				//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 ，如果源图书的点击量大于500，那么就给补全的图书添加上随机百分比，否则就不添加
				if(bookClickMap.containsKey(srcbookId) && bookClickMap.get(srcbookId) > 500) {
					//由于是随机补充百分比，所以我们可以先在while循环外面计算出第一本书的百分比，然后在遇到sequence == 0的条件时，就可以把这本书的百分比添加进去
					
					while(sequence < corelationRecNum && used.size()<hotBookArray.length) {
						int idx = rand.nextInt(hotBookArray.length);
						used.add(idx);
						while(picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
							idx = rand.nextInt(hotBookArray.length);
							used.add(idx);
						}
						if(!picked.contains(hotBookArray[idx].getFirst())){
							String hotBookId = hotBookArray[idx].getFirst();
							picked.add(hotBookId);
							if((seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId)) {
								if(sequence == 0) {
									result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
								} else {
									if(subsequenceLowerBoundRate * confidence > 0.025){
										confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
									}else {
										confidence = rand.nextDouble() * (confidence - 0.025) + 0.025;
									}			
									result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));						
								}
								++sequence;	
								//测试
								//System.out.print("大类补白"+ hotBookId+"|");
							}
						}
					}
				} else {
					
					while (sequence < corelationRecNum && used.size()<hotBookArray.length) {				
						int idx = rand.nextInt(hotBookArray.length);
						used.add(idx);
						while (picked.contains(hotBookArray[idx].getFirst())&& used.size()<hotBookArray.length) {
							idx = rand.nextInt(hotBookArray.length);
							used.add(idx);
						}
						if(!picked.contains(hotBookArray[idx].getFirst())){
							String hotBookId = hotBookArray[idx].getFirst();
							picked.add(hotBookId);
							if((seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(srcbookId) && !freeBookSet.contains(hotBookId)) {
								result.append("|" + hotBookId + "|");
								++sequence;	
								//测试
								//System.out.print("大类补白"+ hotBookId+"|");
							}
						}				
					}
				}
			}
			//System.out.println("补白最终数量"+sequence);
			context.write(new Text(result.toString()), NullWritable.get());
		}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Configuration conf = context.getConfiguration();

		corelationRecNum = conf.getInt("corelation.recommend.number", 10);
		String bookClassFrequencyPath = conf.get("book.class.frequency.path");

		// 排除同系列
		String bookSeriesPath = conf.get("book.series.path");
		int hotBookCount = conf.getInt("select.hotbook.count.for.random", 200);
		// add
		String freeBookPath = conf.get("free.book.path");
		
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表、百分比上限、百分比下限
		String bookClickPath = conf.get("book.click.path");
		firstLowerBound = conf.getFloat("first.lower.bound", 0.10f);
		firstUpperBound = conf.getFloat("first.upper.bound", 0.35f);

		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		for (int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				in = new BufferedReader(
						new FileReader(localFiles[i].toString()));
				if (localFiles[i].toString().contains(bookClassFrequencyPath)) {
					while ((line = in.readLine()) != null) {/* 图书ID|作者|分类|大类|频次 */
						String[] fields = line.split("\\|", -1);
						String bookId = fields[0];
						String author = fields[1];
						String classType = fields[2];
						String bigClass = fields[3];
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
						} else {
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
				} else if (localFiles[i].toString().contains(bookSeriesPath)) {
					while ((line = in.readLine()) != null) {
						String fields[] = line.split("\\|", -1);
						String bookId = fields[0];
						String seriesId = fields[1];
						if (fields.length == 2) {// bookid|series_id
							bookSeriesMap.put(bookId, seriesId);
							if (seriesBookSetMap.containsKey(seriesId)) {
								Set<String> bookSet = seriesBookSetMap
										.get(seriesId);
								bookSet.add(bookId);
							} else {
								Set<String> bookSet = new HashSet<String>();
								bookSet.add(bookId);
								seriesBookSetMap.put(seriesId, bookSet);

							}

						}
					}
				} else if (localFiles[i].toString().contains(freeBookPath)) {
					while ((line = in.readLine()) != null) {
						freeBookSet.add(line.trim());
					}
				} else if(localFiles[i].toString().contains(bookClickPath)) {
					//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 
					while((line = in.readLine()) != null) {
						String fields[] = line.split("\\|", -1);
						if(fields.length == 2) {
							if(!bookClickMap.containsKey(fields[0]))
								bookClickMap.put(fields[0], Long.parseLong(fields[1]));
						}
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}
	}

}
