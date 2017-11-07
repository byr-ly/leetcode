package com.eb.bi.rs.mras2.bookrec.guessyoulike;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.eb.bi.rs.mras2.bookrec.guessyoulike.util.Similar;

public class CalculationOfRelationJoinReducer extends Reducer<Text, Text, NullWritable, Text>{
	private String field_separator = "|"; 
	private String inner_separator = ",";
	
/*	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		//分隔符加载
		field_separator =context.getConfiguration().get("Appconf.data.field.separator");
		inner_separator =context.getConfiguration().get("Appconf.data.inner.separator");
	}*/
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String bookList = "";
		String similarString = "";
		
		Similar oneSimilar = new Similar();
		
		for(Text value: values){
			String[] fields;
			if(field_separator.equals("|")) {
				fields = value.toString().split("\\|");
			} else {
				fields = value.toString().split(field_separator);
			}
			
			bookList = fields[0] + inner_separator + bookList;
			
			oneSimilar.addString(fields[1],inner_separator);
		}
		
		bookList = bookList.substring(0, bookList.length()-inner_separator.length());
		
		similarString = oneSimilar.tosimilarString(inner_separator);
		
		//输出格式:用户|待推荐图书|来源|源图书集|预测评分向量
		context.write(NullWritable.get(),new Text(key.toString()+field_separator+bookList+field_separator+similarString));
	}

}
