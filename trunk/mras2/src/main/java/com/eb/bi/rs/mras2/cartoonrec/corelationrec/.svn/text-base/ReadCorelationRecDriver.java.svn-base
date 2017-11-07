package com.eb.bi.rs.mras2.cartoonrec.corelationrec;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.IndicatorFilterMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.JoinClassFrequencyReducer;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.JoinClassMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.JoinFrequncyMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.ReadCorelationBookRecMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.ReadCorelationBookRecReducer;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.ReadRecForBookWithoutCooccurrenceInfoMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.ReadRecForBookWithoutCooccurrenceInfoReducer;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.CommonUtil.TextPair;

public class ReadCorelationRecDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Logger log = PluginUtil.getInstance().getLogger();
		PluginConfig config = PluginUtil.getInstance().getConfig();		
		
		Configuration conf ;
		Job job;
		FileStatus[] status;
		long start;
		
		
		/*
		 * ********************************************************************************************
		 * 
		 *                                          READ ALSO READ
		 * 
		 * ********************************************************************************************
		 */		
		
		/***********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：深度阅读关联指标数据
		 **     2.缓存文件：图书分类信息
		 **     3.缓存文件：深度阅读频次数据
		 ** 输出：
		 **		满足一定条件约束的关联指标数据
		 *      图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype 
		 ** 功能描述：
		 ** 	过滤关联指标数据，使其满足：
		 **		1.B书的深度阅读用户数大于一定阈值50
		 **     2.AB的共同用户数满足一定阈值5（输入已经满足该条件，见CooccurenceDriver里的参数min_num）

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		
		String bookDeepReadFrequencyPath = config.getParam("book_deep_read_frequency_path", "");
		String readFirsthandIndicatorPath = config.getParam("read_firsthand_indicator_path", "");
		String readFilteredIndicatorPath = config.getParam("read_filtered_indicator_path", "");
		String bookclickPath = config.getParam("book_click_path", "");
		conf = new Configuration(getConf());
		conf.setInt("deep.read.threshold", config.getParam("read_deep_read_threshold", 50));		
		conf.set("book.click.path", bookclickPath);
			
		job = Job.getInstance(conf,"CorelationRecommend-filter_firsthand_indicator_data");

		status = FileSystem.get(conf).globStatus(new Path(bookclickPath+"/*"));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		job.setJarByClass(getClass());
		
		check(readFilteredIndicatorPath);		
		FileInputFormat.setInputPaths(job, new Path(readFirsthandIndicatorPath ));	
		FileOutputFormat.setOutputPath(job, new Path(readFilteredIndicatorPath));
		log.info("job input path: " + readFirsthandIndicatorPath);
		log.info("job output path: " + readFilteredIndicatorPath);		
		
		job.setMapperClass(IndicatorFilterMapper.class);
		job.setNumReduceTasks(0);	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}	
		
		
		//连接的目的是为了在补白时排序拿到同分类下的热书top200
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书分类信息
		 **     2.缓存文件：深度阅读频次数据
		 ** 输出：
		 **		图书分类信息与深度阅读频次信息进行内连接的结果
		 *      book_id动漫ID|author_id作者ID|class_id分类ID|charge_type计费类型|点击量|频次
		 ** 功能描述：
		 ** 	将图书分类信息与深度阅读频次信息进行内连接，

		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		
		String bookInfoPath = config.getParam("book_info_path", "");	
		String bookDeepReadClassFrequencyPath = config.getParam("book_deep_read_class_frequency_path", "");	
		
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");
		
		job = Job.getInstance(conf, "CorelationRecommend-ReadAlsoRead-join_book_class_freqency");
		job.setJarByClass(getClass());		
		
		MultipleInputs.addInputPath(job, new Path(bookInfoPath), TextInputFormat.class, JoinClassMapper.class);		
		MultipleInputs.addInputPath(job, new Path(bookDeepReadFrequencyPath), TextInputFormat.class, JoinFrequncyMapper.class);
		
		check(bookDeepReadClassFrequencyPath);
		FileOutputFormat.setOutputPath(job, new Path(bookDeepReadClassFrequencyPath));
		
		log.info("job input path: " + bookInfoPath);
		log.info("job input path: " + bookDeepReadFrequencyPath);		
		log.info("job output path: " + bookDeepReadClassFrequencyPath);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		job.setReducerClass(JoinClassFrequencyReducer.class);
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(config.getParam("book_deep_read_join_reduce_task_num", 6));
			
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：满足一定条件约束的关联指标数据【第一个job的输出数据】
		 *        图书A|图书B|图书A用户数|图书B用户数|图书AB共同用户数|前置信度|后置信|KULC|IR|classtype 
		 **     2.缓存文件：深度阅读分类频次数据 【第2个job的输出数据】
		 *        book_id动漫ID|author_id作者ID|class_id分类ID|charge_type计费类型|点击量|频次
		 ** 输出：
		 **		为存在关联指标数据的图书筛选关联图书
		 ** 功能描述：    
		 ** 	1.筛选规则
		 **       优先选择Class_type=1的图书， 若图书数不足，则选择class_type=2的图书
		 **     2.补白策略  顺序补充：
		 **		A、	同作者其他漫画； 
		 **		B、	同三级分类（class_id）下热门漫画200，（每次补白随机顺序获取），非大类
		 **		C、	所有漫画随机补白
		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		
		String readCorelationBookRecPath = config.getParam("read_corelation_book_recommend_path", "");
		
		conf = new Configuration(getConf());
		
		conf.setInt("read.corelation.recommend.number", config.getParam("read_corelation_recommend_number", 50));
		conf.set("book.class.frequency.path", bookDeepReadClassFrequencyPath);
		conf.setFloat("limit.bound", config.getParam("limit_bound", 0.085f));
		conf.setFloat("lower.bound", config.getParam("lower_bound", 0.15f));
		conf.setFloat("first.upper.bound1", config.getParam("first_upper_bound1", 0.60f));
		conf.setFloat("first.lower.bound", config.getParam("first_lower_bound", 0.35f));
		conf.setFloat("first.upper.bound", config.getParam("first_upper_bound", 0.70f));
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.setFloat("click.limit", config.getParam("click_limit", 500f));
		
		job = Job.getInstance(conf, "CorelationRecommend-ReadAlsoRead-read_corelation_book_recommend");

		status = FileSystem.get(conf).globStatus(new Path(bookDeepReadClassFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
				
		job.setJarByClass(getClass());
			
		check(readCorelationBookRecPath);
		FileInputFormat.setInputPaths(job, new Path(readFilteredIndicatorPath ));
		FileOutputFormat.setOutputPath(job, new Path(readCorelationBookRecPath));		
			
		log.info("job input path: " + readFilteredIndicatorPath);
		log.info("job output path: " + readCorelationBookRecPath);	
		
		job.setMapperClass(ReadCorelationBookRecMapper.class);
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		job.setReducerClass(ReadCorelationBookRecReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("read_rec_reduce_task_num", 1));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书分类信息
		 **     2.缓存文件：阅读关联图书推荐结果（上一个模块的输出）
		 ** 输出：
		 **		补全的关键推荐结果
		 ** 功能描述：    
		 ** 	上面的模块执行完后，会有如下几种图书没有推荐列表：
		 **     1.没有被订购或者与其他书一起被订购过的图书，即没有出现在订购关联关系中的图书
         **     2.出现在阅读关联关系表中，但是所有记录都不满足 “支持度大于一定阈值且改善度大于一定阈值”的条件
         **     3.出现在阅读关联关系表中，但是其关联图书都下架
         *
		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();
	
		String readRecResultForBookWithoutCooccurrenceInfoPath = config.getParam("read_recresult_for_book_without_cooccurrenceinfo_path", "");
		
		conf = new Configuration(getConf());	
		
		conf.set("corelation.book.recommend.path", readCorelationBookRecPath);
		conf.set("book.class.frequency.path", bookDeepReadClassFrequencyPath);
		
		conf.setInt("corelation.recommend.number", config.getParam("read_corelation_recommend_number", 50));			
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.setFloat("limit.bound", config.getParam("limit_bound", 0.085f));
		conf.setFloat("first.lower.bound", config.getParam("first_lower_bound", 0.35f));
		conf.setFloat("first.upper.bound", config.getParam("first_upper_bound", 0.70f));
		conf.setFloat("click.limit", config.getParam("click_limit", 500f));
		
		job = Job.getInstance(conf, "CorelationRecommend-ReadAlsoRead-recommend_for_book_without_cooccurrence_info");

		status = FileSystem.get(conf).globStatus(new Path(bookDeepReadClassFrequencyPath +"/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf)
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		
		
		status = FileSystem.get(conf).globStatus(new Path(readCorelationBookRecPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("read corelation book recommend result file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		
		
		job.setJarByClass(getClass());
	
		check(readRecResultForBookWithoutCooccurrenceInfoPath);
		FileInputFormat.setInputPaths(job, new Path(bookInfoPath ));
		FileOutputFormat.setOutputPath(job, new Path(readRecResultForBookWithoutCooccurrenceInfoPath));	
		
		log.info("job input path: " + bookInfoPath);
		log.info("job output path: " + readRecResultForBookWithoutCooccurrenceInfoPath);		
		
		job.setMapperClass(ReadRecForBookWithoutCooccurrenceInfoMapper.class);

		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);	

		job.setReducerClass(ReadRecForBookWithoutCooccurrenceInfoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(config.getParam("read_complement_reduce_task_num", 1));
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}		
		return 0;
	}
	
	
	public static void main( String[] args ) throws Exception { 	
    	PluginUtil.getInstance().init(args);	
    	Logger log = PluginUtil.getInstance().getLogger();
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new ReadCorelationRecDriver(), args);	

		Date dateEnd = new Date();


		long timeCost = dateEnd.getTime() - dateBeg.getTime();		

		PluginResult result = PluginUtil.getInstance().getResult();		
		result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
		result.save();
		
		log.info("time cost in total(ms) :" + timeCost) ;
		System.exit(ret);	
    }
	
	
	public void check(String fileName) {
		Logger log = PluginUtil.getInstance().getLogger();
		try {
			FileSystem fs = FileSystem.get(URI.create(fileName),new Configuration());
			Path f = new Path(fileName);
			boolean isExists = fs.exists(f);
			if (isExists) {	//if exists, delete
				boolean isDel = fs.delete(f,true);
				log.info(fileName + "  delete?\t" + isDel);
			} else {
				log.info(fileName + "  exist?\t" + isExists);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	 public static class KeyPartition extends Partitioner<TextPair, TextPair>{
			@Override
			public int getPartition(TextPair key, TextPair value, int numPartitions) {
				return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
			}    	
	 }
	
	

}
