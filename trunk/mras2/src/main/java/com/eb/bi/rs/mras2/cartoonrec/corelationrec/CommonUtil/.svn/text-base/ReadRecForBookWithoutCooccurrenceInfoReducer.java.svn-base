package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadRecForBookWithoutCooccurrenceInfoReducer extends Reducer<TextPair, TextPair, Text, NullWritable> {

	private int corelationRecNum;

	private double firstLowerBound ;
	private double firstUpperBound ;
	private double limitBound ;
	private double clicklimit;
	private HashMap<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private HashMap<String, HashSet<String>> authorBookMap = new HashMap<String, HashSet<String>>();
	private HashMap<String, String>  bookClassMap = new HashMap<String, String>();
	private HashMap<String, String>  bookAuthorMap = new HashMap<String, String>();
	private Set<String> allBookSet = new HashSet<String>();
	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
		    //动漫ID|作者ID|分类ID|计费类型|点击量
	        String [] ff = key.getFirst().toString().split("\\|",-1);
	        String srcbookId = ff[0];
	        String author =ff[1]; 
	        String classType = ff[2];
            Double click = Double.parseDouble(ff[4]);
			StringBuffer result = new StringBuffer(srcbookId);
			int sequence = 0;
			Random rand = new Random();
			double confidence = rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;
			double subsequenceLowerBoundRate;
			if (confidence > 0.5) {
				subsequenceLowerBoundRate = 0.96;			
			} else if(confidence > 0.4) {
				subsequenceLowerBoundRate = 0.965;			
			} else {
				subsequenceLowerBoundRate = 0.97;
			}
			
			Set<String> picked = new HashSet<String>();
			//作家补白
			if (!author.equals("")&& authorBookMap.containsKey(author)) {
				String[] BookArray = authorBookMap.get(author).toArray(new String[] {});
				ArrayList<String> books = new ArrayList<String>();
				for(int i=0; i<BookArray.length;i++){
					books.add(BookArray[i]);
				}
				Collections.shuffle(books);
				Iterator<String> it = books.iterator();
				if(click > clicklimit) {
					while(it.hasNext()&&sequence < corelationRecNum) {
							String BookId = it.next();
							if(!BookId.equals(srcbookId)&&!picked.contains(BookId)) {
								picked.add(BookId);
								if(sequence == 0) {
									result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));
								} else {
									if(subsequenceLowerBoundRate * confidence > limitBound){
										confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
									}else {
										confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
									}			
									result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));
								}
								++sequence;	
								//测试
								//System.out.print("作者补白"+BookId+"|");
						}
					}
				}else{
					while(it.hasNext()&&sequence < corelationRecNum) {
							String BookId = it.next();
							if(!BookId.equals(srcbookId) && !picked.contains(BookId)) {
								picked.add(BookId);
								result.append("|" + BookId + "|");
								++sequence;		
								//测试
								//System.out.print("作者补白"+BookId+"|");
							}			
					}
				}
			}
			//分类热书补白
			if (classHotBookMap.containsKey(classType)) {				
				StringIntPair[] hotBookArray = classHotBookMap.get(classType).toArray(new StringIntPair[] {});
				ArrayList<String> books = new ArrayList<String>();
				for(int i=0; i<hotBookArray.length;i++){
					books.add(hotBookArray[i].getFirst());
				}
				Collections.shuffle(books);
				Iterator<String> it = books.iterator();
				//如果源图书的点击量大于500，那么就给补全的图书添加上随机百分比，否则就不添加
				if(click > clicklimit) {
				   while(it.hasNext()&&sequence < corelationRecNum) {
					    String hotBookId = it.next();
						if(!hotBookId.equals(srcbookId) && !picked.contains(hotBookId)) {
							picked.add(hotBookId);
							if(sequence == 0) {
								result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							} else {
								if(subsequenceLowerBoundRate * confidence > limitBound){
									confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
								}else {
									confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
								}			
								result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							}
							++sequence;	
							//测试
							//System.out.print("分类补白"+ hotBookId+"|");
						}	
					}
				} else {
					while (it.hasNext()&&sequence < corelationRecNum) {	
						String hotBookId = it.next();
						if(!hotBookId.equals(srcbookId) && !picked.contains(hotBookId)) {
							picked.add(hotBookId);
							result.append("|" + hotBookId + "|");
							++sequence;	
							//测试
							//System.out.print("分类补白"+ hotBookId+"|");
						}		
					}
				}
			}
			//全部书补白
			if (sequence < corelationRecNum) {				
				String[] hotBookArray = allBookSet.toArray(new String[] {});
				ArrayList<String> books = new ArrayList<String>();
				for(int i=0; i<hotBookArray.length;i++){
					books.add(hotBookArray[i]);
				}
				Collections.shuffle(books);
				Iterator<String> it = books.iterator();
				
				//如果源图书的点击量大于500，那么就给补全的图书添加上随机百分比，否则就不添加
				if(click > clicklimit) {
					while(it.hasNext()&&sequence < corelationRecNum) {
						String hotBookId = it.next();
						if(!hotBookId.equals(srcbookId) && !picked.contains(hotBookId)) {
							picked.add(hotBookId);
							if(sequence == 0) {
								result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							} else {
								if(subsequenceLowerBoundRate * confidence > limitBound){
									confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
								}else {
									confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
								}			
								result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							}
							++sequence;	
							//测试
							//System.out.print("大类补白"+ hotBookId+"|");
						}
				
					}
				} else {
					while (it.hasNext()&&sequence < corelationRecNum) {				
							String hotBookId = it.next();
							if(!hotBookId.equals(srcbookId) && !picked.contains(hotBookId)) {
								picked.add(hotBookId);
								result.append("|" + hotBookId + "|");
								++sequence;	
								//测试
								//System.out.print("大类补白"+ hotBookId+"|");
							}
						}				
					}
			}
			//System.out.println("补白最终数量"+sequence);
			context.write(new Text(result.toString()), NullWritable.get());
		}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		corelationRecNum = conf.getInt("corelation.recommend.number", 10);
		String bookClassFrequencyPath = conf.get("book.class.frequency.path");
		int hotBookCount = conf.getInt("select.hotbook.count.for.random", 200);
	
		limitBound = conf.getFloat("limit.bound", 0.085f) ;
		firstLowerBound = conf.getFloat("first.lower.bound", 0.35f) ;
		firstUpperBound = conf.getFloat("first.upper.bound", 0.70f) ;
		clicklimit = conf.getFloat("click.limit", 500f) ;
		
		URI[] localFiles = context.getCacheFiles();
		for (int i = 0; i < localFiles.length; ++i) {
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				if(localFiles[i].toString().contains(bookClassFrequencyPath)) {
					while((line = in.readLine()) != null) { /*动漫ID|作者ID|分类ID|计费类型|点击量|频次*/
						String[] fields = line.split("\\|", -1);
						if(fields.length >= 6) {
							String bookId = fields[0];
							String author = fields[1];
							String classType = fields[2];
							String frequency = fields[5];
							//所有漫画
							allBookSet.add(bookId);
							//动漫作者对应表
							if(!author.equals("")){
								bookAuthorMap.put(bookId, author);
							}
							//动漫分类对应表
							bookClassMap.put(bookId, classType);
							//作者书集
							if(!author.equals("")){
								if (authorBookMap.containsKey(author)) {
									HashSet<String> Bookset = authorBookMap.get(author);	
										Bookset.add(bookId);
								}else {
									HashSet<String> Bookset = new HashSet<String>();
									Bookset.add(bookId);
									authorBookMap.put(author, Bookset);							
								}
							}
							//分类热书
							StringIntPair pair = new StringIntPair(bookId,Integer.parseInt(frequency));
							if (classHotBookMap.containsKey(classType)) {
								TreeSet<StringIntPair> hotBookset = classHotBookMap.get(classType);	
								if (hotBookset.size() < hotBookCount) {
									hotBookset.add(pair);
								} else if (pair.compareTo(hotBookset.last()) < 0) {									
									hotBookset.remove(hotBookset.last());
									hotBookset.add(pair);
								}
							}else{
								TreeSet<StringIntPair> hotBookset = new TreeSet<StringIntPair>();
								hotBookset.add(pair);
								classHotBookMap.put(classType, hotBookset);							
							}
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
