package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class AssociatedWordTopology {
	public static void main(String[] args) throws Exception{

		
//		PluginUtil.getInstance().init(args);		
//		Logger logger = PluginUtil.getInstance().getLogger();		
		

		//storm configuration
		Config stormConf= new Config();
		stormConf.put("AppConfig", appConfigStr(PluginUtil.getInstance().getConfig().getConfigFilePath()));		
		stormConf.setNumWorkers(1);
	
		TopologyBuilder builder = new TopologyBuilder();
		String spoutName = new String("msg-spout");
		builder.setSpout(spoutName, new MsgSpout(spoutName),1);
		String KeyWordRecBoltName = new String("key-word-recommend-bolt");
		builder.setBolt(KeyWordRecBoltName, new KeyWordRecBolt(KeyWordRecBoltName),1).shuffleGrouping(spoutName);
		String TagRecBoltName = new String("tag-recommend-bolt");
		builder.setBolt(TagRecBoltName, new TagRecBolt(TagRecBoltName),1).shuffleGrouping(spoutName);
		
		
		String resultCombineBolt = new String("result-combine-bolt");
		builder.setBolt(resultCombineBolt, new ResultCombineBolt(resultCombineBolt),1)
			.fieldsGrouping(KeyWordRecBoltName, new Fields("user","word"))
			.fieldsGrouping(TagRecBoltName, new Fields("user","word"));
		
		
		String saveResultBolt = new String("save-result-bolt");
		builder.setBolt(saveResultBolt, new SaveResultBolt(saveResultBolt),1)
			.shuffleGrouping(resultCombineBolt);					                                               
	    
	    LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Associated-Word-Topology", stormConf, builder.createTopology());
		Thread.sleep(1000);
		cluster.killTopology("Associated-Word-Topology");
		cluster.shutdown();
		
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
