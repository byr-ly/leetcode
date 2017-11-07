package org.mras.bookrec.unifiedinterface;

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
		
		PrintHelper.print("RequestSpout open() begin.");
		
		this.collector = collector;		
		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(conf.get("AppConfig").toString());	
		InputMsgManager inputMsgManager = InputMsgManager.getInstance();
		inputMsgManager.init(appConf);
		dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		while (dataLoader == null) {			
			PrintHelper.print("get data loader error. try again...");
			dataLoader = inputMsgManager.getLoader(context.getThisComponentId());
		}

		PrintHelper.print("RequestSpout open() end.");
	}

	@Override
	public void nextTuple(){
		
		PrintHelper.print("RequestSpout nextTuple() begin.");

		/*When there are no tuples to emit, it is courteous
	     * to have nextTuple sleep for a short amount of time (like a single millisecond)
	     * so as not to waste too much CPU
	     */
		
		DataRecord record = dataLoader.getRecord();
		if(record == null) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			PrintHelper.print("RequestSpout nextTuple() end.");
			return;
		}
		
		//userid=用户ID&edition_id=版面ID
		String user = record.getField("userid").getField();
		String edition = record.getField("edition_id").getField();
		
		if (user.equals("13958080393") || user.equals("13858038966")) {
			edition = "7";
		}
		
		if (edition.isEmpty()) {
			edition = "7";
		}
		
		if (!user.isEmpty() && !edition.isEmpty() ) {
			collector.emit(new Values(user, edition), msgId++);
			PrintHelper.print("emit tuple: [" + user + "," + edition +  "]");
		}	
		
		
//		collector.emit(new Values("user", "1"), msgId++);
//		try {
//			Thread.sleep(100000000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		PrintHelper.print("RequestSpout nextTuple() end.");
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "edition"));
	}

}
