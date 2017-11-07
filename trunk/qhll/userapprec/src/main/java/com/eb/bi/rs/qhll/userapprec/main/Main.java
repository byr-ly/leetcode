package com.eb.bi.rs.qhll.userapprec.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.qhll.userapprec.predictpref.ItemBaseCfDriver;
import com.eb.bi.rs.qhll.userapprec.userapppref.UserAppPrefDriver;

public class Main {

	public static void main(String[] args) throws Exception
	{
		PluginUtil   plugin = PluginUtil.getInstance();
		plugin.init(args);
		PluginConfig config = plugin.getConfig();
		Logger       log    = plugin.getLogger();
		PluginResult result = plugin.getResult();
		
		
		Date dateBegin = new Date();
		//run the jobs
		int ret = ToolRunner.run(new Configuration(), new UserAppPrefDriver(args), args);	

		Date dateEnd = new Date();
		
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(dateEnd);		
		
		long timeCost = dateEnd.getTime() - dateBegin.getTime();
		
		result.setParam("taskNameStart", "user app pref");
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", ret == 0 ? PluginExitCode.PE_SUCC : PluginExitCode.PE_LOGIC_ERR);
		result.setParam("exitDesc", ret == 0 ? "user-app-pref-job run successfully" : "user-app-pref-job run failed.");
		result.setParam("taskNameEnd", "user app pref");
		result.save();
		
		log.info("time cost in total(s) :" + timeCost/1000) ;
			
		
		long startTime = System.nanoTime();
		String hdfs = config.getParam("hdfs", null);
		String inputDataPath = config.getParam("inputDataPath", null);
		String itemidItemnumPath = config.getParam("itemidItemnumPath", null);
		String useridItemnumPrefPath = config.getParam("useridItemnumPrefPath", null);
		String mahoutCfOuputPath = config.getParam("mahoutCfOutputPath", null);
		String mahoutAlgoTempPath = config.getParam("mahoutAlgoTempPath", null);
		String useridItemidPredictPrefPath = config.getParam("outputPath", null);
	
		//mahout
		String booleanData = config.getParam("booleanData", "false");
		String maxSimilarPerItem = config.getParam("maxSimilaritiesPerItem", "300");
		String numRecommemdations = config.getParam("numRecommendations",  "20");
		String similarityClassname = config.getParam("similarityClassname", "org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CosineSimilarity");
		
		String[] params = new String[]{     hdfs, 
				                            inputDataPath, 
				                            itemidItemnumPath, 
				                            useridItemnumPrefPath, 
				                            mahoutCfOuputPath, 
				                            mahoutAlgoTempPath, 
				                            useridItemidPredictPrefPath,
				                            booleanData,
				                            maxSimilarPerItem,
				                            numRecommemdations,
				                            similarityClassname
				                            };
		
		int returnCode = ToolRunner.run(new Configuration(), new ItemBaseCfDriver(), params);
		
		if ((returnCode == 0) && (ret == 0))
		{
			result.setParam("isSuccess", "true");
		}
		else
		{
			result.setParam("isSuccess", "false");
		}
		
		long endTime2 = System.nanoTime();
		long timeCosts = (endTime2 - startTime) / 1000000000;
		result.setParam("timeCosts(s)=", timeCosts + "");
	}
}
