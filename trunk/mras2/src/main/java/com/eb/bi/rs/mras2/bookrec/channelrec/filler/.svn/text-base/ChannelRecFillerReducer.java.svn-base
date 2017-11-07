package com.eb.bi.rs.mras2.bookrec.channelrec.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ChannelRecFillerReducer extends Reducer<Text, Text, NullWritable, Text>  {
	private int fillerNumber;
	private int fillerBooksNumber;
	
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		List<String> books = new ArrayList<String>();

		for (Text value : values) {
			books.add(value.toString());
		}

		for (int i = 0; i < fillerNumber; i++) {
			List<String> fillerRecBookList = generateFiller(books);
			String fillerRecBooks = linkBooks(fillerRecBookList);
			context.write(NullWritable.get(), new Text(i +"_"+ key.toString() + ";" + fillerRecBooks));
			Thread.sleep(1L);
		}
	}

	private String linkBooks(List<String> highlyRecBookList) {
		StringBuilder books = new StringBuilder();

		for (String bookId : highlyRecBookList) {
			books.append(bookId + ",|");
		}		
		
		return books.toString();		
	}

	private List<String> generateFiller(List<String> list) {
		Random random = new Random(System.currentTimeMillis());
		List<String> bookIds = new ArrayList<String>();
		int size = list.size();
		if (size < fillerBooksNumber) {
			bookIds.addAll(list);
		}
		while (bookIds.size() < fillerBooksNumber) {
			int index = random.nextInt(size);
			String bookId = (String) list.get(index);
			if (bookIds.contains(bookId)) {
				index = (index + 1) % size;
				bookId = (String) list.get(index);
			}			
			bookIds.add(bookId);				
		}
		
		return bookIds;
	}

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		fillerNumber = Integer.parseInt(conf.get("fillerNumber"));
		fillerBooksNumber = Integer.parseInt(conf.get("fillerBooksNumber"));
	}

}
