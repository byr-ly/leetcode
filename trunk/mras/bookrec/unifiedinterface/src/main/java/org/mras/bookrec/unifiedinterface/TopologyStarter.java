package org.mras.bookrec.unifiedinterface;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;


public class TopologyStarter {
	
	public static void main(String[] args) {
		
		
		
		Date dateBeg = new Date();
		
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		PluginConfig pluginConfig = pluginUtil.getConfig();
		
		int workerNum = pluginConfig.getParam("worker_num", 1);
		int spoutNum = pluginConfig.getParam("spout_num", 1);
		int boltNum = pluginConfig.getParam("bolt_num", 1);	
		int maxSpoutPending =  pluginConfig.getParam("max_spout_pending", 10000);		
		
		//storm configuration
		Config conf = new Config();
		conf.setMaxSpoutPending(maxSpoutPending);
		conf.setNumWorkers(workerNum);
		conf.setNumAckers(0);
		
		try {
			conf.put("AppConfig", appConfigStr(pluginConfig.getConfigFilePath()));
		} catch (IOException e) {
			e.printStackTrace();
		}		
		
				
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("request-spout", new RequestSpout(), spoutNum );
		builder.setBolt("handle-bolt", new HandleBolt(), boltNum)
			.shuffleGrouping("request-spout");
		
		
		String exitDesc = "run successfully";
		int exitCode = PluginExitCode.PE_SUCC;
		
		try {
			StormSubmitter.submitTopology("unified-interface-topology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			exitDesc = "submit topology already alive";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			exitDesc = "submit topology is invalid";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		}
		
				
		//idox写入运行结果
		Date dateEnd = new Date();
		String endTime = new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd);		
		long timeCost = dateEnd.getTime() - dateBeg.getTime();
		
		PluginResult result = pluginUtil.getResult();		
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", exitCode );
		result.setParam("exitDesc", exitDesc);
		result.save();
		
		
		
//		//Topology definition
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("request-spout", new RequestSpout(), 1 );
//		builder.setBolt("handle-bolt", new HandleBolt(), 1)
//			.shuffleGrouping("request-spout");
//		
//		
//		Config conf = new Config();
//		
//	    LocalCluster cluster = new LocalCluster();
//	    cluster.submitTopology("word-count", conf, builder.createTopology());


		

	}
	
	
	public static String appConfigStr(String confFilePath) throws IOException{
		 String data = "";
		 String line = "";				 
		 BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(confFilePath));
			while((line = br.readLine()) != null ){
				data += line;				 
			}		 
		} finally {
			if (br != null){
				br.close();
			}
		}		 
		return data;
	}

}
