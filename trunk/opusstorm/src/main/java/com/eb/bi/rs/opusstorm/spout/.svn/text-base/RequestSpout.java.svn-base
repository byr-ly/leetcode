package com.eb.bi.rs.opusstorm.spout;

import java.util.Map;

import com.eb.bi.rs.frame2.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame2.common.storm.datainput.DataRecord;
import com.eb.bi.rs.frame2.common.storm.datainput.InputMsgManager;
import com.eb.bi.rs.frame2.common.storm.datainput.LoaderBase;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

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

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				conf.get("AppConfig").toString());
		InputMsgManager inputMsgManager = InputMsgManager.getInstance();
		inputMsgManager.init(appConfig);
		dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		while (dataLoader == null) {
			dataLoader = inputMsgManager
					.getLoader(context.getThisComponentId());
		}
	}

	@Override
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
		String userid = record.getField("userid").getField();
		String type = record.getField("type").getField();
		PrintHelper.print("Receive userid : " + userid + ", type : " + type);

		if (!userid.isEmpty() && !type.isEmpty()) {
			collector.emit(new Values(userid, type));
		} else {
			PrintHelper.print("userid or type is empty");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "type"));
	}

}
