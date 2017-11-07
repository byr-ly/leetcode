package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.MinHeapSortUtil;

//新的需求：
//若type='1'的相似图书不足20本，则用type=‘2’补足20本，type=‘2’的图书KULC全部设置成0.05，尽量保证有书推荐，最多100本
//选取关联图书的时候，直接筛选出推荐池中的图书，

public class SelectBookSimilarityReducer extends Reducer<Text, BookInfo, NullWritable, Text>{
	
	private String similarityIndex;
	private int selectMaxNumber;
	private int selectMinNumber;
	private float complementaryConfidence;
	@Override
	protected void reduce(Text key, Iterable<BookInfo> values, Context context) throws IOException ,InterruptedException {

		ArrayList<BookInfo> preferredList = new ArrayList<BookInfo>();
		ArrayList<BookInfo> secondaryList = new ArrayList<BookInfo>();

		for(BookInfo value : values){
			BookInfo book = new BookInfo(value);			
			if (book.getClassType() == 1) {
				preferredList.add(book);
			} else {
				secondaryList.add(book);
			}
		}

		ArrayList<BookInfo> preferredTopList = MinHeapSortUtil.getTopNArray(preferredList, selectMaxNumber);
		
		//输出格式：图书A|图书B|来源|相似度
		for (BookInfo bookInfo : preferredTopList) {
			context.write(NullWritable.get(), new Text(key.toString() + "|" + bookInfo.getBookId() + "|" + similarityIndex + "|" + bookInfo.getConfidence()));
		}
		
		if (preferredTopList.size() < selectMinNumber) {
			ArrayList<BookInfo> secondaryTopList = MinHeapSortUtil.getTopNArray(secondaryList, selectMinNumber - preferredList.size());	
			for (BookInfo bookInfo : secondaryTopList) {
				context.write(NullWritable.get(), new Text(key.toString() + "|" + bookInfo.getBookId() + "|" + similarityIndex + "|" + complementaryConfidence ));
			}
		}	
	}
	
	@Override
	protected void setup(Context context) throws java.io.IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		similarityIndex = conf.get("similarity.index", "1");
		selectMinNumber = conf.getInt("select.min.number", 20);
		selectMaxNumber = conf.getInt("select.max.number", 100);
		complementaryConfidence = conf.getFloat("complementary.confidence", 0.05f);
	}
}
