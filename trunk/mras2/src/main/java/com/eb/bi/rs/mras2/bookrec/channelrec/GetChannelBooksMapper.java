package com.eb.bi.rs.mras2.bookrec.channelrec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetChannelBooksMapper extends Mapper<Object, Text, Text, Text> {

	private HashMap<String, Set<String>> channel_books = new HashMap<String, Set<String>>();
	private HashMap<String, Set<String>> class_books= new HashMap<String, Set<String>>();
	private HashMap<String, Map<String, Double>> bookSimilars = new HashMap<String , Map<String , Double>>();
	private int channelRecNum;
	private double increaseScore;
	private double maxScore;
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, Double> bookScore = new HashMap<String, Double>();
		Map<String, Integer> bookAssoTimes = new HashMap<String, Integer>();
		Set<String> classes = new HashSet<String>();
		Set<String> historyBooks = new HashSet<String>();
		//输入文件格式：msisdn;channel_type;bookid,classid,score|...;book1,book2...
		String[] fields = value.toString().split(";");
		String msisdn = fields[0];
		String channel_type = fields[1];
		Set<String> books = channel_books.get(channel_type);
		if(books == null){
			return;
		}
		if(fields.length >= 3 && !StringUtils.isBlank(fields[2])){
			String[] booksInfos = fields[2].split("\\|");
			for (String string : booksInfos) {
				String[] bookInfo = string.split(",");
				if (bookInfo.length == 3) {
					String bookid = bookInfo[0];
					String classid = bookInfo[1];
					Double score = Double.parseDouble(bookInfo[2]);
					classes.add(classid);
					historyBooks.add(bookid);
					Map<String, Double> bookSimilar = bookSimilars.get(bookid);
					if (bookSimilar != null) {
						Iterator<String> it = bookSimilar.keySet().iterator();
						while (it.hasNext()) {
							String book = it.next();
							if (books.contains(book)) {
								Double currentScore = bookScore.get(book);
								Double similarScore = score * bookSimilar.get(book);
								if (currentScore == null) {
									bookScore.put(book, similarScore);
									bookAssoTimes.put(book, 1);
								}else if(similarScore > currentScore){
									bookScore.put(book, similarScore);
									bookAssoTimes.put(book, 1+bookAssoTimes.get(book));
								}else {
									bookAssoTimes.put(book, 1+bookAssoTimes.get(book));
								}
							}
						}
					}
				}
			}
		}
		if(fields.length == 4 && !StringUtils.isBlank(fields[2])){
			String[] filters = fields[3].split(",");
			for(String book : filters){
				historyBooks.add(book);
			}
		}
		int count = 0;//计数用户待推荐图书的数量
		Set<String> bookSet = new HashSet<String>();
		bookSet.addAll(bookScore.keySet());
		bookSet.removeAll(historyBooks);
		Iterator<String> it = bookSet.iterator();
		while (it.hasNext()) {
			count++;
			String book = it.next();
			Double similarScore = bookScore.get(book);
			int time = bookAssoTimes.get(book);
			similarScore = similarScore + increaseScore*(time - 1);
			similarScore = similarScore < maxScore ? similarScore : maxScore;
			context.write(new Text(msisdn + "_" + channel_type), new Text(book + "|" + similarScore));
		}
		Set<String> fillerBooks = new HashSet<String>();
		if (count < channelRecNum) {
			Set<String> booktemp = new HashSet<String>();
			for (String classId : classes) {
				Set<String> bookIds = class_books.get(classId);
				if (bookIds != null) {
					booktemp.addAll(bookIds);									
				}
			}
			booktemp.removeAll(bookSet);
			booktemp.removeAll(historyBooks);
			fillerBooks = getFillerBooks(channelRecNum - count , booktemp);
			for (String book : fillerBooks) {
				count++;
				context.write(new Text(msisdn + "_" + channel_type), new Text(book + "|" + 0.0));
			}
		}
		if (count < channelRecNum) {
			if (fillerBooks != null) {
				bookSet.addAll(fillerBooks);					
			}
			Set<String> booktemp = new HashSet<String>();
			for (String book : books) {
				if (!(bookSet.contains(book) || historyBooks.contains(book))) {
					booktemp.add(book);									
				}
			}
			fillerBooks = getFillerBooks(channelRecNum - count , booktemp);
			for (String book : fillerBooks) {
				context.write(new Text(msisdn + "_" + channel_type), new Text(book + "|" + 0.0));
			}
		}
	}

	private Set<String> getFillerBooks(int num , Set<String> books) {
		int size = books.size();
		Set<String> set = new HashSet<String>();
		if (size <= num) {
			set .addAll(books);
		}else {
			List<String> bookList = new ArrayList<String>();
			for (String book : books) {
				bookList.add(book);
			}
			Random random = new Random(System.currentTimeMillis());
			while (num > 1) {
				int index = random.nextInt(size);
				String fillerBook = bookList.get(index);
				while (set.contains(fillerBook)) {
					index = (index + 1) % size;
					fillerBook = bookList.get(index);
				}
				set.add(fillerBook);
				num--;
			}			
		}
		return set;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		channelRecNum = Integer.parseInt(conf.get("channelRecNum"));
		increaseScore = Double.parseDouble(conf.get("increaseScore"));
		maxScore = Double.parseDouble(conf.get("maxScore"));
		String bookSimilarInfo = conf.get("bookSimilarInfo");
		String channelRecBooksInfo = conf.get("channelRecBooksInfo");
		//Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		URI[] localFiles = context.getCacheFiles();
		for(int i = 0; i < localFiles.length; i++) {
			String line;
			BufferedReader in = null;
			try {
				//Path path = new Path(localFiles[i].getPath());
				//in = new BufferedReader(new FileReader(path.getName().toString()));

				FileSystem fs = FileSystem.get(localFiles[i], conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[i]))));
				System.out.println(localFiles[i].toString());

				if (localFiles[i].toString().contains(bookSimilarInfo)) {
					while((line = in.readLine()) != null) { //图书Id|图书Id|相似度				
						String[] fields = line.split("\\|");
						if (fields.length == 3) {
							Double similar = Double.parseDouble(fields[2]);
							classifySimilar(fields[0], fields[1], similar);
						}					
					}								
				}
				if (localFiles[i].toString().contains(channelRecBooksInfo)) {
					while((line = in.readLine()) != null) { //channel_type|bookid|classid|book_stype				
						String[] fields = line.split("\\|");
						if (fields.length == 4 && !StringUtils.isBlank(fields[1])) {
							addBook(fields[0] , fields[1] , channel_books);
							addBook(fields[2] , fields[1] , class_books);
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
	
	private void classifySimilar(String srcBid , String simialrBid , Double similar){
		if (bookSimilars.get(srcBid) == null) {
			HashMap<String, Double> map = new HashMap<String , Double>();
			map.put(simialrBid, similar);
			bookSimilars.put(srcBid, map);
		}else {
			bookSimilars.get(srcBid).put(simialrBid, similar);
		}
	}
	
	private void addBook(String key , String bid , Map<String , Set<String>> map ){
		Set<String> set = map.get(key);
		if (set == null) {
			set = new HashSet<String>();
		}
		set.add(bid);
		map.put(key, set);
	}
	
}
