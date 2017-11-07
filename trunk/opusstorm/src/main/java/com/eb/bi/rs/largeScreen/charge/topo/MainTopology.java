package com.eb.bi.rs.largeScreen.charge.topo;

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
import backtype.storm.tuple.Fields;

import com.eb.bi.rs.frame2.common.pluginutil.PluginResult;
import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig;
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode;
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil;
import com.eb.bi.rs.largeScreen.charge.Constant;
import com.eb.bi.rs.largeScreen.charge.bolt.CityBolt;
import com.eb.bi.rs.largeScreen.charge.bolt.FailReasonBolt;
import com.eb.bi.rs.largeScreen.charge.bolt.SplitBolt;
import com.eb.bi.rs.largeScreen.charge.bolt.SummaryBolt;

public class MainTopology {
	static Logger log = Logger.getLogger(MainTopology.class);
	
	public static void main(String[] args) throws Exception  {
		
		//读取Storm配置文件
		Date dateBeg = new Date();
		PluginUtil pluginUtil = PluginUtil.getInstance();
		pluginUtil.init(args);
		PluginConfig pluginConfig = pluginUtil.getConfig();

		int workerNum = pluginConfig.getParam("worker_num", 1);
		int spoutNum = pluginConfig.getParam("spout_num", 1);
		int splitBoltNum = pluginConfig.getParam("split_bolt_num", 1);
		int cityBoltNum = pluginConfig.getParam("city_bolt_num", 1);
		int summaryBoltNum = pluginConfig.getParam("summary_bolt_num", 1);
		int failReasonBoltNum = pluginConfig.getParam("fail_reason_bolt_num", 1);
		int ackerNum = pluginConfig.getParam("acker_num", 5);
		int maxSpoutPending = pluginConfig.getParam("max_spout_pending", 1000);

		String zkCfg = pluginConfig.getParam("zkCfg", "");
		String largeScreen_topic = pluginConfig.getParam("largeScreen_topic",
				"mgchgf");
		String zkRoot = pluginConfig.getParam("zkRoot", "/ebupt-largeScreen");
		
		//设置storm运行参数
		Config config = new Config();
		config.setMaxSpoutPending(maxSpoutPending);
		config.setNumWorkers(workerNum);
		config.setNumAckers(ackerNum);
		config.setMessageTimeoutSecs(60);
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
		if (largeScreen_topic.equals("")) {
			log.error("Kafka's topic is null.");
			System.exit(1);
		}

		// 构建topo
		ZkHosts zkHosts = new ZkHosts(zkCfg);
		SpoutConfig srcDataSpoutConfig = new SpoutConfig(zkHosts, largeScreen_topic,
				zkRoot, "chargeData");
		srcDataSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		srcDataSpoutConfig.forceFromStart = false;
		srcDataSpoutConfig.socketTimeoutMs = 60000;
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(Constant.KAFKA_SPOUT, new KafkaSpout(srcDataSpoutConfig),spoutNum);
		builder.setBolt(Constant.SPLIT_BOLT, new SplitBolt() , splitBoltNum).shuffleGrouping(Constant.KAFKA_SPOUT);
		builder.setBolt(Constant.CITY_BOLT, new CityBolt(), cityBoltNum).fieldsGrouping(Constant.SPLIT_BOLT , new Fields(Constant.CITY_ID));
		builder.setBolt(Constant.FAIL_REASON_BOLT, new FailReasonBolt(), failReasonBoltNum).globalGrouping(Constant.SPLIT_BOLT);
		builder.setBolt(Constant.SUMMARY_BOLT, new SummaryBolt(), summaryBoltNum).globalGrouping(Constant.SPLIT_BOLT);
		
		String exitDesc = "run successfully";
		int exitCode = PluginExitCode.PE_SUCC;

		try {
			StormSubmitter.submitTopology("largeScreen-topology", config,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			exitDesc = "submit topology already alive";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			exitDesc = "submit topology is invalid";
			exitCode = PluginExitCode.PE_EXEC_ERR;
		}

		// idox写入运行结果
		Date dateEnd = new Date();
		String endTime = new SimpleDateFormat("yyyyMMddHHmmss").format(dateEnd);
		long timeCost = dateEnd.getTime() - dateBeg.getTime();

		PluginResult result = pluginUtil.getResult();
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
