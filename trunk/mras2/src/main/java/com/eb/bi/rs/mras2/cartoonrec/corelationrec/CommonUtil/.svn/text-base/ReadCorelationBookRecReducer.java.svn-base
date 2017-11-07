package com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class ReadCorelationBookRecReducer extends Reducer<TextPair, TextPair, Text, NullWritable>{
	
	private int readCorelationRecNum;

	private double firstLowerBound ;
	private double firstUpperBound ;
	private double limitBound ;
	private double firstLowerBound1 ;
	private double lowerBound ;
	private double clicklimit;
	private HashMap<String, TreeSet<StringIntPair>> classHotBookMap = new HashMap<String, TreeSet<StringIntPair>>();
	private HashMap<String, HashSet<String>> authorBookMap = new HashMap<String, HashSet<String>>();
	private HashMap<String, String>  bookClassMap = new HashMap<String, String>();
	private HashMap<String, String>  bookAuthorMap = new HashMap<String, String>();
	private HashMap<String, Double>  bookClickMap = new HashMap<String, Double>();
	private Set<String> allBookSet = new HashSet<String>();
	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException ,InterruptedException {
		
		/*************************************************************************
		 推书规则：
		1）	优先选择Class_type=1的图书；
		2）	若图书数不足，则选择class_type=2的图书；
		3）	若仍不足，则进行补白，补白规则为：先补同作者，然后分类热书top200，然后全部漫画里随机补白。
		*************************************************************************/
		
		/*********************************************************************
		 *随机规则：
		 *A、	第一本书的的占比，目标值35%-70%：
				若第一本书本身自带的百分比>=15%，则随机一个60%-70%的值作为第一本书的百分比，
				若第一本书没有百分比或者百分比<15%，则随机一个目标值范围（35%-70%）的值作为第一本书的百分比；
		  B、	后续的占比获取方式，上限为前一本书的百分比，下限为MAX（前一本书的百分比*限制比例，8.5%）
		         限制比例：
				若第一本书的占比大于50%，则限制比例为96%；
				若第一本书的占比大于40%，则限制比例为96.5%；
				若第一本书的占比大于30%，则限制比例为97%；
		************************************************************************/
		//图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype
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
		//测试
//		System.out.println(cooccurrenceInfoSet);
		
		String srcbookId = key.getFirst().toString();		
		HashSet<String> recBookSet = new HashSet<String>();
		StringBuffer result = new StringBuffer(srcbookId);
		int sequence = 0;
	
		Random rand = new Random();  double confidence = 0.0;
		Iterator<String> iter = cooccurrenceInfoSet.iterator();
		
		//找到第一本书
		while(iter.hasNext()){
			String[] fields = iter.next().split("\\|",-1);
			String bookId = fields[1];
			if(!bookId.equals(srcbookId)) {
				if(bookClickMap.containsKey(srcbookId)&&bookClickMap.get(srcbookId)>clicklimit){
					
//					System.out.println(srcbookId+"|"+"click:"+bookClickMap.get(srcbookId));
					
					recBookSet.add(bookId);
					confidence = Double.parseDouble(fields[5]);
					if( confidence >= lowerBound){ 
						confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound1) + firstLowerBound1;					
					}else{
						confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;
					}
					result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
					++sequence;
					break;
				}else{
					recBookSet.add(bookId);
					result.append("|" + bookId + "|");
					++sequence;
					break;
				}
			}
		}
		//测试
		//System.out.println("result:"+result);
		
		if(sequence == 0) {//如果全是补白的数据，需要生成first confidence数据
			confidence =  rand.nextDouble() * (firstUpperBound - firstLowerBound) + firstLowerBound;	
		}
		double subsequenceLowerBoundRate;
		if (confidence > 0.5) {
			subsequenceLowerBoundRate = 0.96;			
		} else if(confidence > 0.4) {
			subsequenceLowerBoundRate = 0.965;			
		} else {
			subsequenceLowerBoundRate = 0.97;
		}	
		//选取共现图书
		while(iter.hasNext()){
			if(sequence >= readCorelationRecNum) break;
			String[] fields = iter.next().split("\\|",-1);
			String bookId = fields[1];
			if(!bookId.equals(srcbookId)) {
				if(bookClickMap.containsKey(srcbookId)&&bookClickMap.get(srcbookId)>clicklimit){
					
					//System.out.println(srcbookId+"|"+"click:"+bookClickMap.get(srcbookId));
				
					recBookSet.add(bookId);	
					if(subsequenceLowerBoundRate * confidence > limitBound){
						confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
					}else {
						confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
					}
					result.append("|" + bookId + "|" + String.format("%.1f%%", 100 * confidence));
					++sequence;
				}else{
					recBookSet.add(bookId);	
					result.append("|" + bookId + "|");
					++sequence;
				}
			}
		}
		//测试
		//System.out.println("result:"+result);
		
		Set<String> picked = new HashSet<String>();
		//同作者补白	
		if (sequence < readCorelationRecNum && bookAuthorMap.containsKey(srcbookId)) {
			String[] BookArray = authorBookMap.get(bookAuthorMap.get(srcbookId)).toArray(new String[]{});
			ArrayList<String> books = new ArrayList<String>();
			for(int i=0; i<BookArray.length;i++){
				books.add(BookArray[i]);
			}
			Collections.shuffle(books);
			Iterator<String> it = books.iterator();
			while(it.hasNext()){
				String BookId = it.next();	
				picked.add(BookId);
				if( !recBookSet.contains(BookId) && !BookId.equals(srcbookId)) {
					if(bookClickMap.containsKey(srcbookId)&&bookClickMap.get(srcbookId)>clicklimit){
						
						//System.out.println(srcbookId+"|"+"click:"+bookClickMap.get(srcbookId));
						
						recBookSet.add(BookId);
						if(subsequenceLowerBoundRate * confidence > limitBound){
							confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
						}
						else {
							confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
						}			
						result.append("|" + BookId + "|" + String.format("%.1f%%", 100 * confidence));
						++sequence;	
					}else{
						recBookSet.add(BookId);
						result.append("|" + BookId + "|");
						++sequence;	
					}
					
					//测试
					//System.out.print("作者补白:"+sequence+"-"+BookId+"|");
				}
				if(sequence >= readCorelationRecNum)  break;
			} 		
		}
		//分类热书补白	
		if (sequence < readCorelationRecNum&&bookClassMap.containsKey(srcbookId)) {
			StringIntPair[] hotBookArray = classHotBookMap.get(bookClassMap.get(srcbookId)).toArray(new StringIntPair[]{});
			ArrayList<String> books = new ArrayList<String>();
			for(int i=0; i<hotBookArray.length;i++){
				books.add(hotBookArray[i].getFirst());
			}
			Collections.shuffle(books);
			Iterator<String> it = books.iterator();
			while(it.hasNext()&&sequence < readCorelationRecNum){	
				String hotBookId = it.next();
				if(!picked.contains(hotBookId)){
					picked.add(hotBookId);
					if( !recBookSet.contains(hotBookId) && !hotBookId.equals(srcbookId)) {
						if(bookClickMap.containsKey(srcbookId)&&bookClickMap.get(srcbookId)>clicklimit){
							
							//System.out.println(srcbookId+"|"+"click:"+bookClickMap.get(srcbookId));
							
							recBookSet.add(hotBookId);
							if(subsequenceLowerBoundRate * confidence > limitBound){
								confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
							}
							else {
								confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
							}			
							result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							++sequence;
						}else{
							recBookSet.add(hotBookId);
							result.append("|" + hotBookId + "|");
							++sequence;
						}
						//测试
						//System.out.print("分类补白"+sequence+"-"+ hotBookId+"|");
					}
				}				
			}		
		}
		//全部书补白
		if (sequence < readCorelationRecNum) {
			String[] hotBookArray = allBookSet.toArray(new String[]{});
			ArrayList<String> books = new ArrayList<String>();
			for(int i=0; i<hotBookArray.length;i++){
				books.add(hotBookArray[i]);
			}
			Collections.shuffle(books);
			Iterator<String> it = books.iterator();
			while(it.hasNext()&&sequence < readCorelationRecNum){		
				String hotBookId = it.next();
				if(!picked.contains(hotBookId)){
					picked.add(hotBookId);
					if( !recBookSet.contains(hotBookId) && !hotBookId.equals(srcbookId)) {
						if(bookClickMap.containsKey(srcbookId)&&bookClickMap.get(srcbookId)>clicklimit){
							recBookSet.add(hotBookId);
							if(subsequenceLowerBoundRate * confidence > limitBound){
								confidence = rand.nextDouble() * (confidence - subsequenceLowerBoundRate * confidence) + subsequenceLowerBoundRate * confidence;
							}
							else {
								confidence = rand.nextDouble() * (confidence - limitBound) + limitBound;
							}			
							result.append("|" + hotBookId + "|" + String.format("%.1f%%", 100 * confidence));
							++sequence;	
						}else{
							recBookSet.add(hotBookId);
							result.append("|" + hotBookId + "|");
							++sequence;	
						}
						//测试
						//System.out.print("大类补白"+sequence+"-"+ hotBookId+"|");
					}			
				}
			}		
		}
		
		//System.out.println("补白最终数量"+sequence);
		context.write(new Text(result.toString()), NullWritable.get());
		
	}
	
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		readCorelationRecNum = conf.getInt("read.corelation.recommend.number", 50);	
	
		limitBound = conf.getFloat("limit.bound", 0.085f) ;
		lowerBound = conf.getFloat("lower.bound", 0.15f) ;
		firstLowerBound1 = conf.getFloat("first.lower.bound1", 0.60f) ;
		firstLowerBound = conf.getFloat("first.lower.bound", 0.35f) ;
		firstUpperBound = conf.getFloat("first.upper.bound", 0.70f) ;	
		clicklimit = conf.getFloat("click.limit", 500f) ;
		
		String bookClassFrequencyPath = conf.get("book.class.frequency.path");		
		int hotBookCount = conf.getInt("select.hotbook.count.for.random", 200);
	
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; ++i) {
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
							String click = fields[4];
							String frequency = fields[5];
							//所有漫画
							allBookSet.add(bookId);
							//点击量表
							bookClickMap.put(bookId, Double.valueOf(click));
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
			}finally {
				if(in != null){
					in.close();
				}
			}
		
		}	
		
	}
	
}
