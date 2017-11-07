package com.eb.bi.rs.mras2.bookrec.personalrec;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class UserTagPreferReducer extends Reducer<Text, UserTagPreferWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<UserTagPreferWritable> values, Context context) throws IOException, InterruptedException {

		final int N = Integer.parseInt(context.getConfiguration().get("conf.top.n", "4"));
		final int M = Integer.parseInt(context.getConfiguration().get("conf.tag.n", "10"));
		boolean rnFlag = "true".equalsIgnoreCase(context.getConfiguration().get("conf.rn.flag", "true"));

		List<UserTagPreferWritable> books = new ArrayList<UserTagPreferWritable>();
		for (UserTagPreferWritable value : values) {
			books.add(new UserTagPreferWritable(value));
		}

		// 将数据按标签偏好值->订购用户数->图书id的优先级从大到小排序
		Collections.sort(books);

		Map<String, List<String>> tagBookMap = new HashMap<String, List<String>>();
		List<String> tags = new ArrayList<String>();
		for (UserTagPreferWritable book : books) {
			String bookId = book.getId();
			String tag = book.getTag();

			if (!tags.contains(tag)) {
				tags.add(tag);
			}

			if (tagBookMap.containsKey(tag)) {
				tagBookMap.get(tag).add(bookId);
			} else {
				List<String> list = new ArrayList<String>();
				list.add(bookId);
				tagBookMap.put(tag, list);
			}
		}

		int cnt = 0;
		Set<String> selectedBooks = new HashSet<String>();
		for (String tag : tags) {
			// 至多输出M个标签
			if (cnt >= M) {
				break;
			}

			List<String> bookList = new ArrayList<String>(tagBookMap.get(tag));
			bookList.removeAll(selectedBooks);

			// 不足N本书的标签不要
			if (bookList.size() < N) {
				continue;
			}

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < N; i++) {
				sb.append("|" + bookList.get(i));
				selectedBooks.add(bookList.get(i));
			}
			cnt++;
			Text keyOut = new Text(key);
			Text valueOut;
			if (rnFlag) {
				valueOut = new Text(tag + "|" + cnt + sb.toString());
			} else {
				valueOut = new Text(tag + sb.toString());
			}
			context.write(keyOut, valueOut);
		}
	}
}

