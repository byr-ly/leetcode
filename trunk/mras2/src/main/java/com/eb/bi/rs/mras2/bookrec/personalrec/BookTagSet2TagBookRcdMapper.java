package com.eb.bi.rs.mras2.bookrec.personalrec;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*399026965|将军;进展慢;闷骚;平民;小白文;*/
public class BookTagSet2TagBookRcdMapper extends Mapper<LongWritable, Text, Text , NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] fields = value.toString().split("\\|");
		if(fields.length == 2){
			String bookId = fields[0];
			String[] tags = fields[1].split(";");
			for(String tag : tags){
				context.write(new Text(tag + "|" + bookId), NullWritable.get());
			}
		}		
	}	
}
