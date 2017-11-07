package com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.bolt;

import java.util.Map;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.datainput.DataRecord;
import com.eb.bi.rs.frame.common.storm.datainput.InputMsgManager;
import com.eb.bi.rs.frame.common.storm.datainput.LoaderBase;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RequestSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private LoaderBase dataLoader;
	private int msgId;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		PluginConfig appConf = ConfigReader.getInstance().initConfig(
				conf.get("AppConfig").toString());
		InputMsgManager inputMsgManager = InputMsgManager.getInstance();
		inputMsgManager.init(appConf);
		dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		while (dataLoader == null) {
			dataLoader = inputMsgManager
					.getLoader(context.getThisComponentId());
		}
	}

	public void nextTuple() {
		DataRecord record = dataLoader.getRecord();
		if (record == null) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}

		// userid=用户ID&edition_id=版面ID&version_id=分版ID
		String user = record.getField("userid").getField();
		String edition = record.getField("edition_id").getField();
		String version = record.getField("version_id").getField();
		PrintHelper.print("userid:" + user + ">> edition_id:" + edition + ">> version_id:" + version);

		if (edition.isEmpty()) {
			edition = "7";
		}
		
		if (version.isEmpty() || (!version.equals("1") && !version.equals("2") && !version.equals("3"))) {
			version = "0";
		}

		if (!user.isEmpty() && !edition.isEmpty()) {
			collector.emit(new Values(user, edition, version), msgId++);
		} else {
			PrintHelper.print("userid or edition_id is empty");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "edition", "version"));
	}
}
