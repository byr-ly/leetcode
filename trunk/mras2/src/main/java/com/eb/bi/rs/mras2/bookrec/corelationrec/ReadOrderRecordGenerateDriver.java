package com.eb.bi.rs.mras2.bookrec.corelationrec;
  
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.mras2.bookrec.corelationrec.ReadOrderRecordGenerate.BookInfoMapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.ReadOrderRecordGenerate.ReadRecords6m2Mapper;
import com.eb.bi.rs.mras2.bookrec.corelationrec.ReadOrderRecordGenerate.ReadRecords6mMapper;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
  
public class ReadOrderRecordGenerateDriver extends Configured implements Tool{
	
	@Override
    public int run(String[] args)throws Exception{
		
		
	  Logger log = PluginUtil.getInstance().getLogger();
	  PluginConfig config = PluginUtil.getInstance().getConfig();
	  Job job;
	  long start;
	  
	  
  
	 /* ********************************************************************************************
	 * 
	 *                            从近6月历史深度阅读记录里过滤出非漫画的阅读记录
	 * 
	 * ********************************************************************************************/
	 
     log.info("=================================================================================");
     start = System.currentTimeMillis();
     
     String readRecords6m = config.getParam("read_records_6m", "");
     String cartoonInfo = config.getParam("cartoon_info_path", "");
     String anticartoon_read_records = config.getParam("anticartoon_read_records_path", "");
     
     Configuration conf = new Configuration(getConf());
     conf.set("cartoon.info.path", cartoonInfo);
     job = Job.getInstance(conf, "anticartoon-read-records-generate");
     job.setJarByClass(getClass());

     FileStatus[] status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
	 for (int i = 0; i < status.length; i++) {
		 job.addCacheFile(URI.create(status[i].getPath().toString()));
		 log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
	 }
		
     check(anticartoon_read_records);
     log.info("job input path: " + readRecords6m);
     log.info("job output path: " + anticartoon_read_records);
     
     FileInputFormat.addInputPath(job, new Path(readRecords6m));
     FileOutputFormat.setOutputPath(job, new Path(anticartoon_read_records)); 
  
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(NullWritable.class);    
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(NullWritable.class);
     
     job.setMapperClass(ReadRecords6mMapper.class);
     job.setNumReduceTasks(0);
     
     if (job.waitForCompletion(true)){
  		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
      }else{
    	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
    	  return 1;
      }
     
     
     /* ********************************************************************************************
 	 * 
 	 *                            从近6月历史订购记录里过滤出非漫画的订购记录
 	 * 
 	 * ********************************************************************************************/
 	 
      log.info("=================================================================================");
      start = System.currentTimeMillis();
      
      String orderRecords6m = config.getParam("order_records_6m", "");
      cartoonInfo = config.getParam("cartoon_info_path", "");
      String anticartoon_order_records = config.getParam("anticartoon_order_records_path", "");
      
      conf = new Configuration(getConf());
      conf.set("cartoon.info.path", cartoonInfo);
      job = Job.getInstance(conf, "anticartoon-order-records-generate");
      job.setJarByClass(getClass());

      status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
 	  for (int i = 0; i < status.length; i++) {
 		 job.addCacheFile(URI.create(status[i].getPath().toString()));
 		 log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
 	  }
 		
      check(anticartoon_order_records);
      log.info("job input path: " + orderRecords6m);
      log.info("job output path: " + anticartoon_order_records);
      
      FileInputFormat.addInputPath(job, new Path(orderRecords6m));
      FileOutputFormat.setOutputPath(job, new Path(anticartoon_order_records)); 
   
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);    
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      
      job.setMapperClass(ReadRecords6mMapper.class);
      job.setNumReduceTasks(0);
      
      if (job.waitForCompletion(true)){
   		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
       }else{
     	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
     	  return 1;
       }
      
      
      /* ********************************************************************************************
   	 * 
   	 *                            从近6月历史浏览记录里过滤出非漫画的浏览记录
   	 * 
   	 * ********************************************************************************************/
   	 
        log.info("=================================================================================");
        start = System.currentTimeMillis();
        
        String browseRecords6m = config.getParam("browse_records_6m", "");
        cartoonInfo = config.getParam("cartoon_info_path", "");
        String anticartoon_browse_records = config.getParam("anticartoon_browse_records_path", "");
        
        conf = new Configuration(getConf());
        conf.set("cartoon.info.path", cartoonInfo);
        job = Job.getInstance(conf, "anticartoon-browse-records-generate");
        job.setJarByClass(getClass());

        status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
   	  for (int i = 0; i < status.length; i++) {
   		 job.addCacheFile(URI.create(status[i].getPath().toString()));
   		 log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
   	  }
   		
        check(anticartoon_browse_records);
        log.info("job input path: " + browseRecords6m);
        log.info("job output path: " + anticartoon_browse_records);
        
        FileInputFormat.addInputPath(job, new Path(browseRecords6m));
        FileOutputFormat.setOutputPath(job, new Path(anticartoon_browse_records)); 
     
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapperClass(ReadRecords6mMapper.class);
        job.setNumReduceTasks(0);
        
        if (job.waitForCompletion(true)){
     		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
         }else{
       	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
       	  return 1;
         }
      /* ********************************************************************************************
   	 * 
   	 *                            从图书信息里过滤出非漫画的图书信息
   	 * 
   	 * ********************************************************************************************/
   	 
        log.info("=================================================================================");
        start = System.currentTimeMillis();
        
        String bookInfo = config.getParam("book_info_path", "");
        cartoonInfo = config.getParam("cartoon_info_path", "");
        String anticartoon_book_info = config.getParam("anticartoon_book_info_path", "");
        
        conf = new Configuration(getConf());
        conf.set("cartoon.info.path", cartoonInfo);
        job = Job.getInstance(conf, "anticartoon-book-info-generate");
        job.setJarByClass(getClass());

        status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
	   	for (int i = 0; i < status.length; i++) {
	   	   job.addCacheFile(URI.create(status[i].getPath().toString()));
	   	   log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
	    }
   		
        check(anticartoon_book_info);
        log.info("job input path: " + bookInfo);
        log.info("job output path: " + anticartoon_book_info);
        
        FileInputFormat.addInputPath(job, new Path(bookInfo));
        FileOutputFormat.setOutputPath(job, new Path(anticartoon_book_info)); 
     
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapperClass(BookInfoMapper.class);
        job.setNumReduceTasks(0);
        
        if (job.waitForCompletion(true)){
     		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
         }else{
       	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
       	  return 1;
         }
        
        /* ********************************************************************************************
    	 * 
    	 *                            从近6月历史阅读记录里过滤出漫画的阅读记录
    	 * 
    	 * ********************************************************************************************/
    	 
         log.info("=================================================================================");
         start = System.currentTimeMillis();
         
         readRecords6m = config.getParam("read_records_6m2", "");
         cartoonInfo = config.getParam("cartoon_info_path", "");
         String cartoon_read_records = config.getParam("cartoon_read_records_path", "");
         
         conf = new Configuration(getConf());
         conf.set("cartoon.info.path", cartoonInfo);
         job = Job.getInstance(conf, "cartoon-read-records-generate");
         job.setJarByClass(getClass());

         status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
    	 for (int i = 0; i < status.length; i++) {
    		 job.addCacheFile(URI.create(status[i].getPath().toString()));
    		 log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
    	 }
    		
         check(cartoon_read_records);
         log.info("job input path: " + readRecords6m);
         log.info("job output path: " + cartoon_read_records);
         
         FileInputFormat.addInputPath(job, new Path(readRecords6m));
         FileOutputFormat.setOutputPath(job, new Path(cartoon_read_records)); 
      
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(NullWritable.class);    
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(NullWritable.class);
         
         job.setMapperClass(ReadRecords6m2Mapper.class);
         job.setNumReduceTasks(0);
         
         if (job.waitForCompletion(true)){
      		log.info("job[" + job.getJobID() + "] complete, time consumed(ms): " + (System.currentTimeMillis() - start));
          }else{
        	  log.error("job[" + job.getJobID() + "] failed, time consumed(ms): " + (System.currentTimeMillis() - start));
        	  return 1;
          }
         
         
         /* ********************************************************************************************
     	 * 
     	 *                            从近6月历史订购记录里过滤出漫画的订购记录
     	 * 
     	 * ********************************************************************************************/
     	 
          log.info("=================================================================================");
          start = System.currentTimeMillis();
          
          orderRecords6m = config.getParam("order_records_6m", "");
          cartoonInfo = config.getParam("cartoon_info_path", "");
          String cartoon_order_records = config.getParam("cartoon_order_records_path", "");
          
          conf = new Configuration(getConf());
          conf.set("cartoon.info.path", cartoonInfo);
          job = Job.getInstance(conf, "cartoon-order-records-generate");
          job.setJarByClass(getClass());

          status = FileSystem.get(conf).globStatus(new Path(cartoonInfo+"/*"));
     	  for (int i = 0; i < status.length; i++) {
     		 job.addCacheFile(URI.create(status[i].getPath().toString()));
     		 log.info("cartoon info file: " + status[i].getPath().toString() + " has been add into distributed cache");			
     	  }
     		
          check(cartoon_order_records);
          log.info("job input path: " + orderRecords6m);
          log.info("job output path: " + cartoon_order_records);
          
          FileInputFormat.addInputPath(job, new Path(orderRecords6m));
          FileOutputFormat.setOutputPath(job, new Path(cartoon_order_records)); 
       
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(NullWritable.class);    
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(NullWritable.class);
          
          job.setMapperClass(ReadRecords6m2Mapper.class);
          job.setNumReduceTasks(0);
          
          if (job.waitForCompletion(true)){
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
      
     int ret = ToolRunner.run(new ReadOrderRecordGenerateDriver(), args);
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
