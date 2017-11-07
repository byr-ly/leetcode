

///*399026965|将军;进展慢;闷骚;平民;小白文;*/
//public class BookTagSet2TagBookRcdDriver extends Mapper<LongWritable, Text, Text , NullWritable>{
//	@Override
//	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
//		String[] fields = value.toString().split("\\|");
//		if(fields.length == 2){
//			String bookId = fields[0];
//			String[] tags = fields[1].split(",");
//			for(String tag : tags){
//				context.write(new Text(tag + "|" + bookId), NullWritable.get());
//			}
//		}		
//	}	
//}


package com.eb.bi.rs.mras.bookrec.personalrec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.eb.bi.rs.frame.recframe.base.BaseDriver;

public class BookTagSet2TagBookRcdDriver extends BaseDriver {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, getClass().getSimpleName());
		job.setJarByClass(getClass());		
		
		job.setMapperClass(BookTagSet2TagBookRcdMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		/*可以用，分隔*/
		String inputPath = properties.getProperty("input.path");
		FileInputFormat.setInputPaths(job, inputPath);
		String outputPath = properties.getProperty("output.path");
		check(outputPath, conf);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public void check(String path, Configuration conf) 
	{		
		try {			
			FileSystem fs = FileSystem.get(conf);
			fs.deleteOnExit(new Path(path));
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}       
    }
}

