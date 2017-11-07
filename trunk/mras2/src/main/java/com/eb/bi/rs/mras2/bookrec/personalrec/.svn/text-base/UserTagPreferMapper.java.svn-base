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

import org.apache.hadoop.mapreduce.Mapper;

/**
 * 根据用户的标签偏好推荐热门图书
 */
public class UserTagPreferMapper extends Mapper<Object, Text, Text, UserTagPreferWritable> {

	private Map<String, List<UserTagPreferWritable>> userBookMap = new HashMap<String, List<UserTagPreferWritable>>();

	/**
	 * @param value tag|user|prefer|book|order
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\|");
		if (fields.length != 5) {
			return;
		}

		String user = fields[1];
		long order = Long.parseLong(fields[4]);
		double prefer = Double.parseDouble(fields[2]);
		UserTagPreferWritable book = new UserTagPreferWritable(fields[0], fields[3], order, prefer);

		context.write(new Text(user), book);

		//if (userBookMap.containsKey(user)) {
		//	userBookMap.get(user).add(book);
		//} else {
		//	List<UserTagPreferWritable> books = new ArrayList<UserTagPreferWritable>();
		//	books.add(book);
		//	userBookMap.put(user, books);
		//}
	}

	//@Override
	//public void cleanup(Context context) throws IOException, InterruptedException {
	//	final int N = Integer.parseInt(context.getConfiguration().get("conf.top.n", "4"));

	//	for (Map.Entry<String, List<UserTagPreferWritable>> entry : userBookMap.entrySet()) {
	//		List<UserTagPreferWritable> books = entry.getValue();

	//		// 将数据按标签偏好值->订购用户数->图书id的优先级从大到小排序
	//		Collections.sort(books);

	//		Map<String, Integer> tagCnt = new HashMap<String, Integer>();
	//		Set<String> selectedBooks = new HashSet<String>();
	//		for (UserTagPreferWritable book : books) {
	//			String bookId = book.getId();
	//			// 因为mapper只向reducer输出topN个数据，所以不同标签中重复的书需要保留，
	//			// 以免reducer算得的topN不准确
	//			if (selectedBooks.contains(bookId)) {
	//				context.write(new Text(entry.getKey()), book);
	//				continue;
	//			}

	//			String tag = book.getTag();
	//			if (tagCnt.containsKey(tag)) {
	//				int cnt = tagCnt.get(tag);
	//				if (cnt >= N) {
	//					continue;
	//				}
	//				tagCnt.put(tag, cnt + 1);
	//			} else {
	//				tagCnt.put(tag, 1);
	//			}

	//			context.write(new Text(entry.getKey()), book);
	//			selectedBooks.add(bookId);
	//		}
	//	}
	//}
}
