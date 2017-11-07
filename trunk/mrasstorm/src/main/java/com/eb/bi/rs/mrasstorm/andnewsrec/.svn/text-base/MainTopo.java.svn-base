package com.eb.bi.rs.mrasstorm.andnewsrec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class MainTopo {

	static Logger log = Logger.getLogger(MainTopo.class);

	public static void main(String[] args) {

		/*
		 * 读取Storm相关配置
		 */
		Date dateBeg = new Date();
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		PluginConfig pluginConfig = pluginUtil.getConfig();

		int workerNum = pluginConfig.getParam("worker_num", 1);
		int spoutNum = pluginConfig.getParam("spout_num", 1);
		int boltNum = pluginConfig.getParam("bolt_num", 1);
		int maxSpoutPending = pluginConfig.getParam("max_spout_pending", 10000);

		String zkCfg = pluginConfig.getParam("zkCfg", "");
		String srcnews_topic = pluginConfig.getParam("srcNews_Topic",
				"AndNews.SrcNews");
		String zkRoot = pluginConfig.getParam("zkRoot", "/ebupt-andnews");

		/*
		 * 设置Storm相关配置
		 */
		Config config = new Config();
		config.setMaxSpoutPending(maxSpoutPending);
		config.setNumWorkers(workerNum);
		config.setNumAckers(0);
		config.setMessageTimeoutSecs(60000);
		config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
		config.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);

		try {
			config.put("AppConfig",
					appConfigStr(pluginConfig.getConfigFilePath()));
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (srcnews_topic.equals("")) {
			log.error("Kafka's topic is null.");
			System.exit(1);
		}

		/*
		 * 构建Storm Topology
		 */
		ZkHosts zkHosts = new ZkHosts(zkCfg);
		SpoutConfig srcNewsSpoutConfig = new SpoutConfig(zkHosts, srcnews_topic,
				zkRoot, "srcNews");
		srcNewsSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		srcNewsSpoutConfig.forceFromStart = false;
		srcNewsSpoutConfig.socketTimeoutMs = 60000;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka_spout", new KafkaSpout(srcNewsSpoutConfig),
				spoutNum);
		builder.setBolt("andnews_bolt", new AndNewsBolt(), boltNum)
				.shuffleGrouping("kafka_spout");
		
		String exitDesc = "run successfully";
		int exitCode = PluginExitCode.PE_SUCC;
		try {
			StormSubmitter.submitTopology("unifyrec_andnews_topology", config, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			exitDesc = "submit topology alreay alive";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			exitDesc = "submit topology is invalid";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		}
		
		/*
		 * idox写入运行结果
		 */
		Date dateEnd = new Date();
		String endTime = new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd);
		long timeCost = dateEnd.getTime() - dateBeg.getTime();
		
		PluginResult result =  pluginUtil.getResult();
		result.setParam("endTime", endTime);
		result.setParam("timeCosts", timeCost);
		result.setParam("exitCode", exitCode);
		result.setParam("exitDesc", exitDesc);
		result.save();
	}

	public static String appConfigStr(String confFilePath) throws IOException {
		String data = "";
		String line = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(confFilePath));
			while ((line = br.readLine()) != null) {
				data += line;
			}
		} finally {
			if (br != null) {
				br.close();
			}
		}
		return data;
	}
}
