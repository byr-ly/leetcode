package com.eb.bi.rs.mras2.cartoonrec.corelationrec;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.algorithm.occurrence.ConfidenceLevelManager;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;

public class CooccurenceDriver extends Configured implements Tool{
	
	private static PluginUtil pluginUtil;
	private static Logger logger;

	@Override
	public int run(String[] args) throws Exception { 
				
		PluginConfig config =  pluginUtil.getConfig();	
		String inputPath = config.getParam("input_hdfs_path", "");
		String outputPath = config.getParam("output_hdfs_path", "");
		String workPath = config.getParam("work_hdfs_path", "");
		Configuration conf = getConf();
		
		rmr(outputPath,conf);
		rmr(workPath, conf);		
	    /*
	     * 其中min_num是两个动漫之间共同用户数的下限。max_item_num是一个用户的历史漫画的保留数量的上限。neighbour_num是动漫1的相关的动漫的数量上限。
	     */
		return ConfidenceLevelManager.ConfidenceLevel(args, logger, getConf(), 
					inputPath, outputPath, workPath, config.getParam("reduce_num_job", 1), 
					config.getParam("tagdict_max_split_size_job", 64),  config.getParam("KULC_threshold", "0.05") , config.getParam("IR_threshold","0.7"), 
					config.getParam("min_num", "5"),config.getParam("neighbour_num", "1000"),config.getParam("max_item_num", "5000"),config.getParam("hive_input_field_delimiter", "\\|"));
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		
		pluginUtil = PluginUtil.getInstance();
    	pluginUtil.init(args);
		logger = pluginUtil.getLogger();
		
		Date dateBeg = new Date();
		
		int ret = ToolRunner.run(new CooccurenceDriver(), args);
    	
    	Date dateEnd = new Date();
    	
    	SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
    	String endTime = format.format(dateEnd);		
    	long timeCost = dateEnd.getTime() - dateBeg.getTime();
    			
    	PluginResult result = pluginUtil.getResult();		
    	result.setParam("endTime", endTime);
    	result.setParam("timeCosts", timeCost);
    	result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
    	result.setParam("exitDesc", ret == 0 ? "run successfully" : "run failed.");
    	result.save();
    			
    	logger.info("time cost in total(ms) :" + timeCost) ;
    			
    	System.exit(ret);
	}
	
	
   public static void rmr(String folder, Configuration conf) throws IOException 
	{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(conf);
        fs.deleteOnExit(path);
        logger.info("Delete: " + folder);
        fs.close();
    }	

}
