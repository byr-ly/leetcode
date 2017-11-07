package com.eb.bi.rs.mras2.bookrec.personalrec.filler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.recframe.base.BaseDriver;

public class PieceFillerNandHDriver extends BaseDriver {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = Logger.getLogger("PieceFillerNandHDriver");
		long start = System.currentTimeMillis();
		Job job = null;
		Configuration conf;
		conf = new Configuration(getConf());
		
		Properties app_conf = super.properties;
		//配置加载--------------------------------------------------
		//目录配置
		String inputPath = app_conf.getProperty("hdfs.input.path");
		String outputPath = app_conf.getProperty("hdfs.output.path");
		//并行度配置
		int reduceNum = Integer.valueOf(app_conf.getProperty("hadoop.reduce.num"));	
		int maxSplitSizejob = Integer.valueOf(app_conf.getProperty("hadoop.map.maxsplitsizejob"));
		//应用配置
		String recommendNum = app_conf.getProperty("Appconf.piecefiller.recommendnum","12");
		String randomNum = app_conf.getProperty("Appconf.piecefiller.randomnum","500");
		//--------------------------------------------------------
		//并行度配置
		conf.set("mapred.max.split.size", String.valueOf(1024 * 1024 * maxSplitSizejob));
		//应用配置
		conf.set("Appconf.piecefiller.recommendnum",recommendNum);
		conf.set("Appconf.piecefiller.randomnum",randomNum);
		//--------------------------------------------------------
		//job-setup
		job = new Job(conf);
		job.setJarByClass(PieceFillerNandHDriver.class);
		//检查输出目录是否存在
		check(outputPath,conf);
		//设置输入地址
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//设置输出地址
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		//设置M-R
		job.setMapperClass(PieceFillerNandHMapper.class);
		job.setNumReduceTasks(reduceNum);
		job.setReducerClass(PieceFillerNandHReducer.class);
		//设置输入/输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//设置输出类型(map)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型(reduce)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//日志==================================================================================
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		log.info("=================================================================================");			
						
		return 0;
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
	
	public static class PieceFillerNandHMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context ctx) 
				throws IOException, InterruptedException {
			//图书|...(以后可能有)
			String[] fields = value.toString().split("\\|");
			
			ctx.write(new Text(fields[0]),new Text(value.toString()));
		}
	}
	
	public static class PieceFillerNandHReducer extends Reducer<Text, Text, NullWritable, Text> {
		private List<BookStore> fillterList = new ArrayList<BookStore>();
		
		private int recommendNum = 0;
		private int randomNum = 0;
		
		@Override
		protected void setup(Context context
				) throws IOException,InterruptedException {
			recommendNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.recommendnum"));
			randomNum = Integer.valueOf(context.getConfiguration().get("Appconf.piecefiller.randomnum"));
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context
				) throws IOException, InterruptedException{
			//图书|...(以后可能有)
			for(Text value:values){
				fillterList.add(new BookStore(key.toString(),value.toString(),0));
			}
		}
		
		@Override
		protected void cleanup(Context context
	            ) throws IOException, InterruptedException {
			///随机选取补白图书
			//随机randomNum套方案
			for(int i = 0; i != randomNum;i++){
				Set<String> resultSet = new HashSet<String>();			
				//每套方案随机recommendNum本
				if (!fillterList.isEmpty()) {
					for(;;){
						int random = new Random().nextInt(fillterList.size());
						
						resultSet.add(fillterList.get(random).getKey());
						
						if(resultSet.size()==recommendNum){
							break;
						}
					}
				}
				//结果拼接
				String resultString = "";
				Iterator<String> it = resultSet.iterator();
				while (it.hasNext()){
					String entry = it.next();
					//图书,理由(空)|图书,理由(空)|...
					resultString = resultString + entry + "|";
				}
				resultString = resultString.substring(0, resultString.length()-1);
				//写结果
				context.write(NullWritable.get(),new Text(i+"|"+resultString));
				resultSet.clear();
			}
			fillterList.clear();
		}
	}
}
