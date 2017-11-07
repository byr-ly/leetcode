package com.eb.bi.rs.mras2.bookrec.qiangfarec.filler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class SelectHighlyRecommendBooksFillerReducer extends Reducer<Text, Text, NullWritable, Text>  {
	@SuppressWarnings("rawtypes")
	private MultipleOutputs mos;
	private int fillerNumber;
	private int highlyRecBookNum;
	private int topBookNumber;
	protected static String HIGHLYREC = "highlyRec";
	protected static String TOPREC = "topRec";
	
	@SuppressWarnings("unchecked")
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		List<String> books = new ArrayList<String>();

		for (Text value : values) {
			books.add(value.toString());
		}
		
		for (int i = 0; i < fillerNumber; i++) {
			List<String> highlyRecBookList = generateFiller(highlyRecBookNum , books);
			List<String> topRecBookList = generateFiller(topBookNumber , highlyRecBookList);
			String highlyRecBooks = linkBooks(highlyRecBookList);
			String topRecBooks = linkBooks(topRecBookList);
			mos.write(HIGHLYREC, NullWritable.get(),new Text(i + "_" + key.toString() + ";" + highlyRecBooks),"zone_" + HIGHLYREC + "/part");
			mos.write(TOPREC, NullWritable.get(),new Text(i + "_" + key.toString() + ";" + topRecBooks),"zone_" + TOPREC + "/part");
			Thread.sleep(1L);
		}
	}

	private String linkBooks(List<String> highlyRecBookList) {
		StringBuffer books = new StringBuffer();

		for (String bookId : highlyRecBookList) {
			books.append(bookId + "|");
		}		
		
		return books.toString();		
	}

	private List<String> generateFiller(int number, List<String> list) {
		Random random = new Random(System.currentTimeMillis());
		List<String> bookIds = new ArrayList<String>();

		while (bookIds.size() < number) {			
			int index = random.nextInt(list.size());
			String bookId = list.get(index);
			if (!bookIds.contains(bookId)) {
				bookIds.add(bookId);				
			}			
		}
		
		return bookIds;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		mos = new MultipleOutputs(context);
		fillerNumber = Integer.parseInt(conf.get("filler.number"));
		highlyRecBookNum = Integer.parseInt(conf.get("highly.recommend.book.number"));
		topBookNumber = Integer.parseInt(conf.get("top.book.number"));
	}
	
	@Override	
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
