package com.eb.bi.rs.mras2.unifyrec.similarcategory;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarCategoryMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text book = new Text();  
	private Text score = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] splits = value.toString().split("\\|", -1);
		if(splits.length>=2){
				book.set(splits[0]);
				score.set(splits[1]);
			
		}
		context.write(book,score);
	}

}
