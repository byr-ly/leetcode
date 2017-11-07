package com.eb.bi.rs.mras2.bookrec.knowledgecal.bookmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewUserhisReducer extends Reducer<Text, Text, Text, Text> {
	private BooksInfo booksInfo = new BooksInfo();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// 加载图书打分类型数据

		Map<String, String> book_type = new HashMap<String, String>();
		Map<String, String> book_readnum = new HashMap<String, String>();

		super.setup(context);
		Configuration conf = context.getConfiguration();
		URI[] localFiles = context.getCacheFiles();

		System.out.println("mapred.cache.localFiles "
				+ conf.getStrings(
						"mapred.cache.localFiles"));
		System.out.println("mapred.cache.files "
				+ conf.getStrings("mapred.cache.files"));

		for (URI path : localFiles) {
			String line;
			BufferedReader in = null;
			try {
				FileSystem fs = FileSystem.get(path, conf);
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
				// 图书-打分类型
				if (path.toString().contains("bookTYPE")) {
					while ((line = in.readLine()) != null) {
						// [0]图书id[1]type
						String fields[] = line.split("\\|");
						book_type.put(fields[0], fields[1]);
					}
				}
				// 图书-用户阅读数
				if (path.toString().contains("bookreadnum")) {
					while ((line = in.readLine()) != null) {
						// [0]图书id[1]阅读用户数
						String fields[] = line.split("\\|");
						book_readnum.put(fields[0], fields[1]);
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}

		Iterator<Entry<String, String>> it = book_type.entrySet().iterator();
		String bookId;
		String Type;
		String Readnum;
		while (it.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) it
					.next();
			bookId = entry.getKey();
			Type = entry.getValue();

			if (!book_readnum.containsKey(bookId)){
				continue;
			}
			Readnum = book_readnum.get(bookId);

			booksInfo.add(bookId, Type, Readnum);
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {// 用户对一本图书的
			// [0]图书id[1]阅读章节数[2]总章节数
			String[] field = value.toString().split("\\|");

			// 图书总章节数
			int book_chnum = Integer.valueOf(field[2]);

			// 内存获得
			int book_TYPE = booksInfo.getBooktype(field[0]);

			if (book_TYPE == -1){// 用户阅读的图书没有type
				continue;
			}
			int readNum = booksInfo.getBookreadnum(field[0]);

			// 阅读章节数
			float bookreadCH = Integer.valueOf(field[1]);

			float bookMpoint = 0;

			// 判定书否被阅读数小于50
			if (readNum < 50 || book_TYPE == 1 || book_TYPE == 2
					|| book_TYPE == 5 || book_TYPE == 8 || book_TYPE == 9) {
				bookMpoint = 5 * bookreadCH / book_chnum;
			} else {// book_TYPE为3,4,6,7且被阅读数>=50
				if (book_TYPE == 3) {
					if (bookreadCH <= 8) {
						bookMpoint = (float) (0.125 * bookreadCH);
					} else if (bookreadCH > 8 && bookreadCH <= 20) {
						bookMpoint = (float) (1.0 / 3 + 1.0 / 12 * bookreadCH);
					} else if (bookreadCH > 20 && bookreadCH <= 0.55 * book_chnum) {// 带log表达式
						bookMpoint = (float) (2.0 - Math.log(20.0) / Math.log(StrictMath.pow(0.03 * book_chnum, 1.0 / 3)) + 
								Math.log(bookreadCH) / Math.log(StrictMath.pow(0.03 * book_chnum, 1.0 / 3)));
					} else if (bookreadCH > 0.55 * book_chnum) {
						bookMpoint = 5;
					}
				} else if (book_TYPE == 4) {
					if (bookreadCH <= 8) {
						bookMpoint = (float) (0.125 * bookreadCH);
					} else if (bookreadCH > 8 && bookreadCH <= 20) {
						bookMpoint = (float) (3.0 / 4 + 1.0 / 32 * bookreadCH);
					} else if (bookreadCH > 20 && bookreadCH <= 50) {// 带log表达式
						bookMpoint = (float) (1.5 - Math.log(20.0)
								/ Math.log(StrictMath.pow(2.5, 2.0)) + Math
								.log(bookreadCH)
								/ Math.log(StrictMath.pow(2.5, 2.0)));
					} else if (bookreadCH > 50 && bookreadCH <= 0.5 * book_chnum) {
						bookMpoint = (float) (1.5 / (0.5 * book_chnum - 50) + (1.75 * book_chnum - 250)
								/ (0.5 * book_chnum - 50));
					} else if (bookreadCH > 0.5 * book_chnum) {
						bookMpoint = 5;
					}
				} else if (book_TYPE == 6) {
					if (bookreadCH == 1) {
						bookMpoint = 0;
					} else if (bookreadCH > 1 && bookreadCH <= 15) {// 带log表达式
						bookMpoint = (float) (Math.log(bookreadCH) / Math
								.log(StrictMath.pow(15.0, 2.0)));
					} else if (bookreadCH > 15 && bookreadCH <= 0.7 * book_chnum) {
						bookMpoint = (float) (3.0 / (0.7 * book_chnum - 15.0) + (1.4 * book_chnum - 75.0)
								/ (0.7 * book_chnum - 15.0));
					} else if (bookreadCH > 0.7 * book_chnum) {
						bookMpoint = 5;
					}
				} else if (book_TYPE == 7) {
					if (bookreadCH <= 6) {
						bookMpoint = (float) 1.0 / 6 * bookreadCH;
					} else if (bookreadCH > 6 && bookreadCH <= 50) {// 带log表达式
						bookMpoint = (float) (1.0 - Math.log(6.0)
								/ Math.log(StrictMath.pow(25.0 / 3.0, 0.4)) + Math
								.log(bookreadCH)
								/ Math.log(StrictMath.pow(25.0 / 3.0, 0.4)));
					} else if (bookreadCH > 50 && bookreadCH <= 0.6 * book_chnum) {// 带log表达式
						bookMpoint = (float) (3.5 - Math.log(50.0) / 
								Math.log(StrictMath.pow(0.012 * book_chnum, 2.0 / 3)) + 
								Math.log(bookreadCH) / 
								Math.log(StrictMath.pow(0.012 * book_chnum, 2.0 / 3)));
					} else if (bookreadCH > 0.6 * book_chnum) {
						bookMpoint = 5;
					}
				}
			}

			// 用户主体得分结果
			if (bookMpoint > 5) {
				bookMpoint = 5;
			}
			if (bookMpoint < 0) {
				bookMpoint = 0;
			}
			// 用户id|图书id|阅读章节数|打分类型|主体得分
			context.write(key, new Text(field[0] + "|" + bookreadCH + "|"
					+ book_TYPE + "|" + bookMpoint));
		}
	}
}
