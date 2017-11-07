package com.eb.bi.rs.mras2.cartoonrec.corelationrec;
  
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.OrderFilterRead.OrderRecResultMapper;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.OrderFilterRead.OrderRecReultFilterReducer;
import com.eb.bi.rs.mras2.cartoonrec.corelationrec.OrderFilterRead.ReadRecResultMapper;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
  
public class OrderCorelationResultFilterDriver extends Configured implements Tool{
	
	@Override
    public int run(String[] args)throws Exception{
		
		
	  Logger log = PluginUtil.getInstance().getLogger();
	  PluginConfig config = PluginUtil.getInstance().getConfig();
	  Job job;
	  long start;
	  
	  
  
	 /* ********************************************************************************************
	 * 
	 *                            订购还订购推荐结果过滤阅读还阅读top10
	 * 
	 * ********************************************************************************************/
	  
	  
	 /*************************************************************************************************
		* MAP REDUCE JOB:
		** 输入：
		**		1.输入路径：订购还订购推荐结果
		**                  阅读还阅读推荐结果
		** 输出：
		**		过滤了阅读还阅读top10的订购还订购推荐结果
		** 功能描述：
	    ** 	      过滤订购还订购关联结果，使其满足：
		**		      同一用户的阅读还阅读推荐结果的top10不在订购还订购推荐结果里
		**
	 *************************************************************************************************/ 
     log.info("=================================================================================");
     start = System.currentTimeMillis();
      
     String orderRecResult = config.getParam("order_corelation_book_recommend_path_unfilter", "");
     String readRecResult = config.getParam("read_corelation_book_recommend_path", "");
     String readRecResultWithoutOccurrence = config.getParam("read_recresult_for_book_without_cooccurrenceinfo_path", "");
     
     Configuration conf = new Configuration(getConf());
      
     conf.setInt("order.corelation.recommend.number", config.getParam("order_corelation_recommend_number",60));
     conf.setInt("order.filter.read.topN", config.getParam("order_filter_read_top_N", 10));
      
     job = Job.getInstance(conf, "order-without-cooccurrence-corelation-rec-result-filter");
     job.setJarByClass(getClass());
      
     MultipleInputs.addInputPath(job, new Path(readRecResult), TextInputFormat.class, ReadRecResultMapper.class);
     MultipleInputs.addInputPath(job, new Path(orderRecResult), TextInputFormat.class, OrderRecResultMapper.class);
     MultipleInputs.addInputPath(job, new Path(readRecResultWithoutOccurrence), TextInputFormat.class, ReadRecResultMapper.class);
      
     String orderFilterResult = config.getParam("order_corelation_book_recommend_path", "");
     check(orderFilterResult);
      
     FileOutputFormat.setOutputPath(job, new Path(orderFilterResult));
     
     log.info("job input path: " + readRecResult);
     log.info("job input path: " + readRecResultWithoutOccurrence);
     log.info("job input path: " + orderRecResult);
     log.info("job output path: " + orderFilterResult);
      
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(Text.class);
      
     job.setReducerClass(OrderRecReultFilterReducer.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(NullWritable.class);
     job.setNumReduceTasks(config.getParam("order_rec_filter_reduce_task_num", 100));
     
     if (job.waitForCompletion(true)){
  		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
      }else{
    	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
    	  return 1;
      }
     
     
     /*************************************************************************************************
		* MAP REDUCE JOB:
		** 输入：
		**		1.输入路径：没有订购关系的用户的订购还订购推荐结果
		**                  阅读还阅读推荐结果
		** 输出：
		**		过滤了阅读还阅读top10的订购还订购推荐结果
		** 功能描述：
	    ** 	      过滤订购还订购关联结果，使其满足：
		**		      同一用户的阅读还阅读推荐结果的top10不在订购还订购推荐结果里
		**
	 *************************************************************************************************/ 
     log.info("=================================================================================");
     start = System.currentTimeMillis();
      
     conf = new Configuration(getConf());
     conf.setInt("order.corelation.recommend.number", config.getParam("order_corelation_recommend_number", 60));
     conf.setInt("order.filter.read.topN", config.getParam("order_filter_read_top_N", 10));
      
     job = Job.getInstance(conf, "order-corelation-rec-result-filter");
     job.setJarByClass(getClass());
      
     readRecResult = config.getParam("read_corelation_book_recommend_path", "");
     String orderwithoutcooccurrenceinfoRecResult = config.getParam("order_recresult_for_book_without_cooccurrenceinfo_path_unfilter", "");
     readRecResultWithoutOccurrence = config.getParam("read_recresult_for_book_without_cooccurrenceinfo_path", "");
      
     MultipleInputs.addInputPath(job, new Path(readRecResult), TextInputFormat.class, ReadRecResultMapper.class);
     MultipleInputs.addInputPath(job, new Path(orderwithoutcooccurrenceinfoRecResult), TextInputFormat.class, OrderRecResultMapper.class);
     MultipleInputs.addInputPath(job, new Path(readRecResultWithoutOccurrence), TextInputFormat.class, ReadRecResultMapper.class);
      
     String orderwithoutcooccurrenceinfoFilterResult = config.getParam("order_recresult_for_book_without_cooccurrenceinfo_path", "");
     check(orderwithoutcooccurrenceinfoFilterResult);
      
     FileOutputFormat.setOutputPath(job, new Path(orderwithoutcooccurrenceinfoFilterResult));
     
     log.info("job input path: " + readRecResult);
     log.info("job input path: " + readRecResultWithoutOccurrence);
     log.info("job input path: " + orderwithoutcooccurrenceinfoRecResult);
     log.info("job output path: " + orderwithoutcooccurrenceinfoFilterResult);
      
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(Text.class);
      
     job.setReducerClass(OrderRecReultFilterReducer.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(NullWritable.class);
     job.setNumReduceTasks(config.getParam("order_rec_filter_reduce_task_num", 100));
     
     if (job.waitForCompletion(true)) {
         log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
      }else{
         log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
         return 1;
      }
         return 0;
    }
    
    public static void main(String[] args)throws Exception{
      PluginUtil.getInstance().init(args);
     Logger log = PluginUtil.getInstance().getLogger();
     Date dateBeg = new Date();
      
     int ret = ToolRunner.run(new OrderCorelationResultFilterDriver(), args);
     Date dateEnd = new Date();

     long timeCost = dateEnd.getTime() - dateBeg.getTime();
      
     PluginResult result = PluginUtil.getInstance().getResult();
     result.setParam("endTime", new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd));
     result.setParam("timeCosts", Long.valueOf(timeCost));
     result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
     result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
     result.save();  
     
     log.info("time cost in total(ms) :" + timeCost);
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
    
}
