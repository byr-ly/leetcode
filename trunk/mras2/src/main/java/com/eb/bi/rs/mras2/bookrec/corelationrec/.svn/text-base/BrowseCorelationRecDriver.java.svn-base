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
import com.eb.bi.rs.mras2.bookrec.corelationrec.Browse.BrowseCorelationBookRecReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.Browse.BrowseRecForBookWithoutCooccurrenceInfoReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.IndicatorFilterMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinClassFrequencyReducer;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinClassMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.JoinFrequncyMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadCorelationBookRecMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.ReadRecForBookWithoutCooccurrenceInfoMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.CommonUtil.TextPair;


public class BrowseCorelationRecDriver extends Configured implements Tool {

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
		 *                                 BROWSE ALSO BROWSE 
		 * 
		 * ********************************************************************************************
		 */		
		
		 /* 
		  * ********************************************************************************************
		  * MAP REDUCE JOB:
			 ** 输入：
			 **		1.输入路径： 浏览关联指标数据
			 **     2.缓存文件：图书分类信息
			 **     3.缓存文件：浏览频次数据
			 ** 输出：
			 **		满足一定条件约束的关联指标数据
			 ** 功能描述：
			 ** 	过滤关联指标数据，使其满足：
			 **		1.A、B图书同属于一个大类
			 **		2.B书的深度阅读用户数大于一定阈值
			 **     3。AB的共同用户数满足一定阈值（输入已经满足该条件）

			 *******************************************************************************************/
		log.info("=================================================================================");		
		start = System.currentTimeMillis();		
		
		conf = new Configuration(getConf());
		
		
		String bookInfoPath = config.getParam("book_info_path","" );
		String bookSeriesPath = config.getParam("book_series_path", "");
		String freeBookPath = config.getParam("free_book_path", "");
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表 
		String bookClickPath = config.getParam("book_click_path", "");

		conf.set("book.click.path", bookClickPath);
		conf.setDouble("deep.read.threshold", config.getParam("browse_deep_read_threshold", 100));	
		conf.set("book.info.path", bookInfoPath);
		conf.set("book.series.path", bookSeriesPath);
		

		job = new Job(conf,"filter firsthand indicator data");

		status = FileSystem.get(conf).listStatus(new Path(bookInfoPath));
		for (int i = 0; i < status.length; i++) {
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
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
		
		String browseFirsthandIndicatorPath = config.getParam("browse_firsthand_indicator_path", "");
		String browseFilteredIndicatorPath = config.getParam("browse_filtered_indicator_path", "");
		check(browseFilteredIndicatorPath);		

		FileInputFormat.setInputPaths(job, new Path(browseFirsthandIndicatorPath));	
		FileOutputFormat.setOutputPath(job, new Path(browseFilteredIndicatorPath));
		log.info("job input path: " + browseFirsthandIndicatorPath);
		log.info("job output path: " + browseFilteredIndicatorPath);		
		
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
		
		
		/*
		 * *************************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书分类信息
		 **     2.缓存文件：浏览频次数据
		 ** 输出：
		 **		图书分类信息与浏览频次信息进行内连接的结果
		 ** 功能描述：
		 ** 	将图书分类信息与浏览频次信息进行内连接，

		 **************************************************************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();	
		
		conf = new Configuration(getConf());
		conf.set("mapred.textoutputformat.separator","|");			
		job = new Job(conf, "join book class and freqency");
		job.setJarByClass(getClass());
		
		String bookDeepReadFrequencyPath = config.getParam("book_deep_read_frequency_path", "");
		String bookBrowseFrequencyPath = config.getParam("book_browse_frequency_path", "");
		String bookBrowseClassFrequencyPath = config.getParam("book_browse_class_frequency_path", "");	
		log.info("job input path: " + bookInfoPath);
		log.info("job input path: " + bookBrowseFrequencyPath);		
		log.info("job output path: " + bookBrowseClassFrequencyPath);
		
		MultipleInputs.addInputPath(job, new Path(bookInfoPath), TextInputFormat.class, JoinClassMapper.class);	
		MultipleInputs.addInputPath(job, new Path(bookBrowseFrequencyPath), TextInputFormat.class, JoinFrequncyMapper.class);
		check(bookBrowseClassFrequencyPath);
		FileOutputFormat.setOutputPath(job, new Path(bookBrowseClassFrequencyPath));
		
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		
		
		job.setReducerClass(JoinClassFrequencyReducer.class);
		job.setNumReduceTasks(config.getParam("book_browse_join_reduce_task_num", 1));
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(Text.class);
			
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
		
		
		/*
		 * ********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：满足一定条件约束的关联指标数据
		 **     2.缓存文件：浏览频次数据
		 **     3.缓存文件：图书分类信息
		 ** 输出：
		 **		为存在关联指标数据的图书筛选关联图书
		 ** 功能描述：    
		 ** 	1.筛选规则
		 **       优先选择Class_type=1的图书， 若图书数不足，则选择class_type=2的图书
		 **     2.补白策略
		 **       同大类下按照置信度或者图书本身热度降序补足，补白图书无置信度数据（选择了图书本身热度降序），增加了随机的逻辑

		 *******************************************************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();			
		//参数修整 2016-10-21  关联推荐百分比优化需求： 随机百分比补充   增加百分比的上限和下限 
		float firstLowerBound = config.getParam("first_lower_bound", 0.35f);
		float firstUpperBound = config.getParam("first_upper_bound", 0.70f);
		float limitBound = config.getParam("limit_bound", 0.085f);
		
		conf = new Configuration(getConf());
		conf.setInt("read.corelation.recommend.number", config.getParam("browse_corelation_recommend_number", 10));		
		conf.set("book.class.frequency.path", bookBrowseClassFrequencyPath);												  
		//参数修整2016-10-21
		conf.setFloat("limit.bound", config.getParam("limit_bound", 0.085f));
		conf.setFloat("lower.bound", config.getParam("lower_bound", 0.15f));
		conf.setFloat("first.upper.bound1", config.getParam("first_upper_bound1", 0.60f));
		conf.setFloat("first.lower.bound", config.getParam("first_lower_bound", 0.35f));
		conf.setFloat("first.upper.bound", config.getParam("first_upper_bound", 0.70f));	
		
		conf.set("book.series.path", bookSeriesPath);
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.set("free.book.path", freeBookPath);

		job = new Job(conf, "browse corelation book recommend");

		status = FileSystem.get(conf).globStatus(new Path(bookBrowseClassFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("browse class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
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
													
		FileInputFormat.setInputPaths(job, new Path(browseFilteredIndicatorPath));
		String browseCorelationBookRecPath = config.getParam("browse_corelation_book_recommend_path", "");
		check(browseCorelationBookRecPath);
		FileOutputFormat.setOutputPath(job, new Path(browseCorelationBookRecPath));		
		
		log.info("job input path: " + browseFilteredIndicatorPath);
		log.info("job output path: " + browseCorelationBookRecPath);		
		
		job.setMapperClass(ReadCorelationBookRecMapper.class);
		job.setReducerClass(BrowseCorelationBookRecReducer.class);
		job.setNumReduceTasks(config.getParam("browse_rec_reduce_task_num", 1));
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);	
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		if( job.waitForCompletion(true)){
			log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
		}
		else {
			log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
			return 1;
		}
					
		/*
		 * ********************************************************************************************
		 * MAP REDUCE JOB:
		 ** 输入：
		 **		1.输入路径：图书分类信息
		 **     2.缓存文件：浏览关联图书推荐结果（上一个模块的输出）
		 ** 输出：
		 **		补全的关联推荐结果
		 ** 功能描述：    
		 ** 	上面的模块执行完后，会有如下几种图书没有推荐列表：
		 **     1.没有被浏览或者与其他书一起被浏览过的图书，即没有出现在订购关联关系中的图书
         **     2.出现在浏览关联关系表中，但是所有记录都不满足 过滤的条件
         **     3.出现在关关联关系表中，但是其关联图书都下架
		 *******************************************************************************************/		
		log.info("=================================================================================");
		start = System.currentTimeMillis();
		
		conf = new Configuration(getConf());		
		
		conf.setInt("corelation.recommend.number", config.getParam("browse_corelation_recommend_number", 10));	
													
		conf.set("corelation.book.recommend.path", browseCorelationBookRecPath);

		conf.set("book.class.frequency.path", bookBrowseClassFrequencyPath);
		conf.set("book.series.path", bookSeriesPath);
		conf.setInt("select.hotbook.count.for.random", config.getParam("select_hotbook_count_for_random", 200));
		conf.set("free.book.path", freeBookPath);
		//chang 关联推荐百分比优化需求： 随机百分比补充   增加图书点击量表  数据格式为bookId|点击量
		conf.set("book.click.path", bookClickPath);	
		conf.setFloat("first.lower.bound", firstLowerBound);
		conf.setFloat("first.upper.bound", firstUpperBound);
		conf.setFloat("limit.bound",limitBound);
		
		job = new Job(conf, "recommend for book without cooccurrence info");

		status = FileSystem.get(conf).globStatus(new Path(bookBrowseClassFrequencyPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("browse class frequency file: " + status[i].getPath().toString() + " has been add into distributed cache");			
		}		

		status = FileSystem.get(conf).globStatus(new Path(browseCorelationBookRecPath + "/part-*"));
		for (int i = 0; i < status.length; i++) {
			//DistributedCache.addCacheFile(URI.create(status[i].getPath().toString()), conf);
			job.addCacheFile(URI.create(status[i].getPath().toString()));
			log.info("browse corelation book recommend result file: " + status[i].getPath().toString() + " has been add into distributed cache");			
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
			log.info("book click path: " + status[i].getPath().toString() + "has been add into distributed cache");
		}
		
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(bookInfoPath));
		String browseRecResultForBookWithoutCooccurrenceInfoPath = config.getParam("browse_recresult_for_book_without_cooccurrenceinfo_path", "");
		check(browseRecResultForBookWithoutCooccurrenceInfoPath);
		FileOutputFormat.setOutputPath(job, new Path(browseRecResultForBookWithoutCooccurrenceInfoPath));		
	
		
		log.info("job input path: " + bookInfoPath);
		log.info("job output path: " + browseRecResultForBookWithoutCooccurrenceInfoPath);		
		
		job.setMapperClass(ReadRecForBookWithoutCooccurrenceInfoMapper.class);
		job.setReducerClass(BrowseRecForBookWithoutCooccurrenceInfoReducer.class);
		job.setNumReduceTasks(config.getParam("browse_complement_reduce_task_num", 1));
		job.setPartitionerClass(KeyPartition.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);	
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
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
		
		int ret = ToolRunner.run(new BrowseCorelationRecDriver(), args);	

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
