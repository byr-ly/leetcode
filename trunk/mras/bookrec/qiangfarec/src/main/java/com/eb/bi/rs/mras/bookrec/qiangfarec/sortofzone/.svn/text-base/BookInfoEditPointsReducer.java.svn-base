package com.eb.bi.rs.mras.bookrec.qiangfarec.sortofzone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BookInfoEditPointsReducer extends Reducer<Text, Text, NullWritable, Text> {

	private String fieldDelimiter;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		fieldDelimiter = conf.get("field.delimiter", "\\|");
	}

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {

		String bookid = "";
		List<String> output = new ArrayList<String>();

		for (Text pair : values) {
			String[] fields = pair.toString().split(fieldDelimiter);
			if (fields.length == 1) { // 长度为1，则数据是图书信息
				bookid = fields[0];
			}
			if (fields.length == 3 && fields[0] != null && fields[1] != null && fields[2] != null) { // 长度为3，且都不为空，则数据是专区图书表处理后的信息
				output.add(pair.toString());
			}
		}

		for(int i = 0; i < output.size(); i++){
			String[] outputInfo = output.get(i).split(fieldDelimiter);
			if(!output.equals("") && !bookid.equals("") && bookid.equals(outputInfo[1])){
				// 专区id | bookid |编辑分
				context.write(NullWritable.get(), new Text(output.get(i)));
			}
		}

	}
}
