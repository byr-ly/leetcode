package com.eb.bi.rs.mras.bookrec.correrecrealtimefilter;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.datainput.DataRecord;
import com.eb.bi.rs.frame.common.storm.datainput.InputMsgManager;
import com.eb.bi.rs.frame.common.storm.datainput.LoaderBase;

public class RequestSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	private LoaderBase dataLoader;	
	private int msgId;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {		
		
		PrintHelper.print("yueqian RequestSpout open() begin.");
		
		this.collector = collector;		
		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(conf.get("AppConfig").toString());	
		InputMsgManager inputMsgManager = InputMsgManager.getInstance();
		inputMsgManager.init(appConf);
		dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		while (null == dataLoader) {			
			PrintHelper.print("get data loader error. try again...");
			dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		}

		PrintHelper.print("yueqian RequestSpout open() end.");
	}

	@Override
	public void nextTuple(){
		DataRecord record = dataLoader.getRecord();
		if(record == null) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		String user = record.getField("userid").getField();
		String book = record.getField("bookid").getField();
		String type = record.getField("type").getField();
		PrintHelper.print("yueqian Spout Receive Tuple: [" + user + "," + book + "," + type  + "]");
		String author;
		if (record.getField("authorid") == null) {
			author = "-1";
		} else {
			author = record.getField("authorid").getField();
		}
		String classid;
		if (record.getField("classid") == null) {
			classid = "-1";
		} else {
			classid = record.getField("classid").getField();
		}
		
		// 异常值处理
		if (user == null || book == null || type == null ) {
			PrintHelper.print("Input field is null");
			return;
		}
	
		if (!user.isEmpty() && !book.isEmpty() && !type.isEmpty()&& !author.isEmpty()&& !classid.isEmpty()) {
			collector.emit(new Values(user, book, type,author,classid), msgId++);
			PrintHelper.print("yueqian Spout Emit Tuple: [" + user + "," + book + "," + type  + ","+author + ","+classid+"]");
		}	

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "book", "type","author","classid"));
	}

}
