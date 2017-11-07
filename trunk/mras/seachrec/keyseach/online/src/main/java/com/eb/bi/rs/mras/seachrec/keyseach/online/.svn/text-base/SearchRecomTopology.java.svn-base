package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
//import com.eb.bi.rs.frame.common.storm.datainput.InputMsgManager;
//import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
//import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;

public class SearchRecomTopology {
	public static void main(String[] args) throws Exception{
		
		PluginUtil pluginUtil;
		
		pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		
		//PluginUtil.getInstance().init(args);
		String app_confpath = pluginUtil.getConfig().getConfigFilePath();//获取配置文件地址
		Logger m_logger = pluginUtil.getLogger();
		
		Date dateBeg = new Date();
		
		m_logger.info("SearchRecomTopology open success! ");
		
		Config conf = new Config();
		
		String appConf = appConfigStr(app_confpath);
		
		conf.put("AppConfig", appConf);
		
		//===========================================================================
		PluginConfig m_appConf;
		m_appConf = ConfigReader.getInstance().initConfig(appConf);
		int workerNum = Integer.valueOf((String) m_appConf.getParam("worker_num"));
		int spoutNum = Integer.valueOf((String) m_appConf.getParam("spout_num"));
		int boltNum1 = Integer.valueOf((String) m_appConf.getParam("bolt_num1"));
		int boltNum2 = Integer.valueOf((String) m_appConf.getParam("bolt_num2"));
		int boltNum3 = Integer.valueOf((String) m_appConf.getParam("bolt_num3"));
		//===========================================================================
		
		conf.setNumWorkers(workerNum);
		
		TopologyBuilder builder = new TopologyBuilder();
		String spoutName = new String("input");
		builder.setSpout(spoutName, new MsgPretreat(spoutName),spoutNum);
		String boltName1 = new String("weight");
		builder.setBolt(boltName1, new WeightCompute(boltName1),boltNum1).shuffleGrouping(spoutName);
		String boltName2 = new String("pref");
		builder.setBolt(boltName2, new PrefManage(boltName2),boltNum2).shuffleGrouping(boltName1);
		String boltName3 = new String("result");
		builder.setBolt(boltName3, new ResultProcess(boltName3),boltNum3).shuffleGrouping(boltName2);
		
		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		
		//任务已提交
		m_logger.info("SearchRecomTopology put on storm !");
		
		//idox写入运行结果
		Date dateEnd = new Date();
		SimpleDateFormat format	 = new SimpleDateFormat("yyyyMMddHHmmss");
		String endTime = format.format(dateEnd);		
		long timeCost = dateEnd.getTime() - dateBeg.getTime();
		
		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", PluginExitCode.PE_SUCC );
		result.setParam("exitDesc", "run on");
		result.save();
		
		System.exit(0);
		
		/*本地模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(args[0], conf, builder.createTopology());
	    Utils.sleep(100000);
	    cluster.killTopology(args[0]);
	    cluster.shutdown();
	    */
	}
	
	public static String appConfigStr(String confpath) throws IOException{
		 String data = "";
		 String line = "";
		 
		 String path = confpath;
		 
		 BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		 while((line = br.readLine()) != null ){
			 data += line;
		 }
		 
		 br.close();
		 
		 return data;
	}
}
