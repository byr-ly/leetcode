package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.util.ArrayList;
//import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;

//import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PrefManage extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8415132319578090046L;
	private String m_name;
	
	private Logger m_logger;
	private TopologyContext m_context;
	private long m_lastTime;
	//private long m_interval;
	private Jedis m_redis = null;
	private int m_receiveCnt;
	private long m_emitCnt;
	
	private int m_number;
	
	private int m_prefcom_flag;//�Ƿ��Ȩ�ؼ���
	
	public PrefManage(String boltName2) {
		// TODO Auto-generated constructor stub
		m_name = boltName2;
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		
		m_context = context;
		
//		LogUtil.getInstance().init(m_context.getStormId(), String.valueOf(m_context.getThisTaskId()), m_name);
//		m_logger = LogUtil.getInstance().getLogger();
		
		//m_interval = Integer.valueOf( (String)conf.get("log_interval"));
		
		String appConfig = (String) conf.get("AppConfig");
		PluginConfig pluginConf = ConfigReader.getInstance().initConfig(appConfig);
		
		String number = pluginConf.getParam("rec_num");
		m_number = Integer.valueOf(number).intValue();
		
		String flag = pluginConf.getParam("flag");
		m_prefcom_flag = Integer.valueOf(flag).intValue();
		
		initRedis(pluginConf);
		System.out.println("bolt's PrefManage prepare success !");//test
	}

	private void initRedis(PluginConfig pluginConf) {
		// TODO Auto-generated method stub
		String addr= pluginConf.getParam("redis_link");
		
		//m_logger.info("PrefManage initRedis is" + addr);//test
		
		String addrs[] = addr.split("::");
		int port = Integer.valueOf(addrs[1]).intValue();
		String ip = addrs[0];
		m_redis = new Jedis(ip, port);
		System.out.println("bolt's PrefManage init redis success !");//test
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's PrefManage begin : " + m_lastTime);
		
		m_receiveCnt++;
		
		String user = input.getStringByField("user");
		String word = input.getStringByField("word");
		String type = input.getStringByField("type");
		String tags_key = input.getStringByField("tags_key");
		String tags_val = input.getStringByField("tags_val");
		
//		m_logger.debug("bolt's PrefManage " + user + " : " + word + " : " + type);//test
//		m_logger.debug("bolt's PrefManage " + tags_key + " : " + tags_val);//test
		
		PrefCompute prefCompute = new PrefCompute(user,word,type,tags_key,tags_val,m_number,m_logger);
		prefCompute.compute(m_redis,m_prefcom_flag);
		
		ArrayList<String> bookresult = prefCompute.getbookresult();
		
//		m_logger.debug("bolt's PrefManage bookresult is : " + bookresult);//test
		
		if(bookresult != null){
			m_emitCnt++;
			collector.emit(new Values(user,bookresult));
		}
		else{
//			m_logger.debug("bolt's PrefManage no bookresult is : " + user+" | "+word);//test
		}
		
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's PrefManage end : " + m_lastTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("user","books"));
	}
	
	@Override
	public void cleanup() {
	    
	}
}
