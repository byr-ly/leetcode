package com.eb.bi.rs.mras2.bookrec.corelationrec;

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
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.IndicatorFilterMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinClassFrequencyReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinClassMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinFrequncyMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadCorelationBookRecMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadCorelationBookRecReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadRecForBookWithoutCooccurrenceInfoMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadRecForBookWithoutCooccurrenceInfoReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.TextPair;

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
		 ** 功能描述：
		 ** 	过滤关联指标数据，使其满足：
		 **		1.A、B图书同属于一个大类
		 **		2.B书的深度阅读用户数大于一定阈值
		 **     3。AB的共同用户数满足一定阈值（输入已经满足该条件）

		 **********************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();	
		
		
		String bookInfoPath = config.getParam("book_info_path","" );
		String bookSeriesPath = config.getParam("book_series_path", "");
		String freeBookPath = config.getParam("free_book_path", "");
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 
		String bookClickPath = config.getParam("book_click_path", "");
		
		conf = new Configuration(getConf());
		conf.setDouble("deep.read.threshold", config.getParam("read_deep_read_threshold", 100));		
		conf.set("book.info.path", bookInfoPath);
		conf.set("book.click.path", bookClickPath);
		conf.set("book.series.path", bookSeriesPath);	

		job = new Job(conf,"CorelationRecommend-filter_firsthand_indicator_data");

		status = FileSystem.get(conf).listStatus(new Path(bookInfoPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("book info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		status = FileSystem.get(conf).listStatus(new Path(bookSeriesPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("book series file: " + status[i].getPath().toString() + " has been add into distributed cache");
		}
		
		status = FileSystem.get(conf).globStatus(new Path(bookClickPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		job.setJarByClass(getClass());
		
		String readFirsthandIndicatorPath = config.getParam("read_firsthand_indicator_path", "");
		String readFilteredIndicatorPath = config.getParam("read_filtered_indicator_path", "");
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
		
		
		
		/****************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书分类信息
		 **     2.缓存文件：深度阅读频次数据
		 ** 输出：
		 **		图书分类信息与深度阅读频次信息进行内连接的结果
		 ** 功能描述：
		 ** 	将图书分类信息与深度阅读频次信息进行内连接，

		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");
		
		job = new Job(conf, "CorelationRecommend-ReadAlsoRead-join_book_class_freqency");
		job.setJarByClass(getClass());		
		String bookDeepReadFrequencyPath = config.getParam("book_deep_read_frequency_path", "");
		String bookDeepReadClassFrequencyPath = config.getParam("book_deep_read_class_frequency_path", "");		
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
		 **		1.输入路径：满足一定条件约束的关联指标数据
		 **     2.缓存文件：深度阅读分类频次数据
		 **     3.缓存文件：图书分类信息
		 ** 输出：
		 **		为存在关联指标数据的图书筛选关联图书
		 ** 功能描述：    
		 ** 	1.筛选规则
		 **       优先选择Class_type=1的图书， 若图书数不足，则选择class_type=2的图书
		 **     2.补白策略
		 **       同二级分类/同三级分类下按照置信度或者图书本身热度降序补足，补白图书无置信度数据（选择了图书本身热度降序）

		 ****************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		
		//参数修整 2016-10-21  关联推荐百分比优化需求： 随机百分比补充   增加百分比的上限和下限 
		float firstLowerBound = config.getParam("first_lower_bound", 0.35f);
		float firstUpperBound = config.getParam("first_upper_bound", 0.70f);
		float limitBound = config.getParam("limit_bound", 0.085f);
		
		conf = new Configuration(getConf());
		conf.setInt("read.corelation.recommend.number", config.getParam("read_corelation_recommend_number", 20));
		conf.set("book.class.frequency.path", bookDeepReadClassFrequencyPath);
		//参数修整2016-10-21
		conf.setFloat("limit.bound", config.getParam("limit_bound", 0.085f));
		conf.setFloat("lower.bound", config.getParam("lower_bound", 0.15f));
		conf.setFloat("first.upper.bound1", config.getParam("first_upper_bound1", 0.60f));
		conf.setFloat("first.lower.bound", config.getParam("first_lower_bound", 0.35f));
		conf.setFloat("first.upper.bound", config.getParam("first_upper_bound", 0.70f));
		
		conf.set("book.series.path", bookSeriesPath);
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.set("free.book.path", freeBookPath);

		job = new Job(conf, "CorelationRecommend-ReadAlsoRead-read_corelation_book_recommend");

		status = FileSystem.get(conf).globStatus(new Path(bookDeepReadClassFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		status = FileSystem.get(conf).listStatus(new Path(bookSeriesPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("book series file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		status = FileSystem.get(conf).listStatus(new Path(freeBookPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("free book path: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
				
		job.setJarByClass(getClass());
			
		String readCorelationBookRecPath = config.getParam("read_corelation_book_recommend_path", "");
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
		
		conf = new Configuration(getConf());		
		
		conf.setInt("corelation.recommend.number", config.getParam("read_corelation_recommend_number", 10));		
		conf.set("corelation.book.recommend.path", readCorelationBookRecPath);
		conf.set("book.class.frequency.path", bookDeepReadClassFrequencyPath);		
		conf.set("book.series.path", bookSeriesPath);
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.set("free.book.path", freeBookPath);
		
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表  数据格式为bookId|点击量
		conf.set("book.click.path", bookClickPath);	
		conf.setFloat("first.lower.bound", firstLowerBound);
		conf.setFloat("first.upper.bound", firstUpperBound);
		conf.setFloat("limit.bound",limitBound);
		
		job = new Job(conf, "CorelationRecommend-ReadAlsoRead-recommend_for_book_without_cooccurrence_info");

		status = FileSystem.get(conf).globStatus(new Path(bookDeepReadClassFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("deep read class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		
		
		status = FileSystem.get(conf).globStatus(new Path(readCorelationBookRecPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("read corelation book recommend result file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		
		
		status = FileSystem.get(conf).listStatus(new Path(bookSeriesPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("book series file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		status = FileSystem.get(conf).listStatus(new Path(freeBookPath));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("free book path: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}
		
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表  
		status = FileSystem.get(conf).listStatus(new Path(bookClickPath));
		for(int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("book click path: " + status[i].getPath().toString() + " has been add into distributed cache");
		}
		
		job.setJarByClass(getClass());
		
		String readRecResultForBookWithoutCooccurrenceInfoPath = config.getParam("read_recresult_for_book_without_cooccurrenceinfo_path", "");
		check(readRecResultForBookWithoutCooccurrenceInfoPath);

		FileInputFormat.setInputPaths(job, new Path(bookInfoPath ));
		FileOutputFormat.setOutputPath(job, new Path(readRecResultForBookWithoutCooccurrenceInfoPath));	
		
		log.info("job input path: " + bookInfoPath);
		
		log.info("job output path: " + readRecResultForBookWithoutCooccurrenceInfoPath);		
		
		//chang 关联推荐百分比优化需求：去除了过滤订购还订购推荐结果的代码
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
