package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.io.UnsupportedEncodingException;
//import java.util.List;
import java.util.Map;

//import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

//import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
//import com.eb.bi.rs.frame.common.storm.datainput.DataField;
import com.eb.bi.rs.frame.common.storm.datainput.DataRecord;
import com.eb.bi.rs.frame.common.storm.datainput.InputMsgManager;
import com.eb.bi.rs.frame.common.storm.datainput.LoaderBase;

//import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;

//import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MsgPretreat extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1638359638590881483L;
	
	private String m_name;
	private SpoutOutputCollector m_collector;
	private TopologyContext m_context;
	
	private PluginConfig m_appConf;
//	private Logger m_logger;
	private Jedis m_redis = null;
	private Jedis m_out_redis = null;
	private LoaderBase m_loader = null;
	private long m_lastTime;
	private long m_interval;
	private int m_receiveCnt;
	private long m_emitCnt;
	
	public MsgPretreat(String name){
		m_name = name;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
		m_context = context;
		m_collector = collector;
		String appConfig = (String) conf.get("AppConfig");
		
		
		//日志初始化
//		LogUtil.getInstance().init(m_context.getStormId(), String.valueOf(m_context.getThisTaskId()), m_name);
//		m_logger = LogUtil.getInstance().getLogger();
		
		
		m_appConf = ConfigReader.getInstance().initConfig(appConfig);
		
		InputMsgManager.getInstance().init(m_appConf);
		m_loader = InputMsgManager.getInstance().getLoader(m_name);
		
		initRedis(m_appConf);
		init_output_Redis(m_appConf);
		
		m_interval = Integer.valueOf((String) m_appConf.getParam("log_interval"));
		
		m_lastTime = System.currentTimeMillis();
		m_receiveCnt = 0;
		m_emitCnt = 0;
		
		System.out.println("spout's MsgPretreat open success!");
	}

	private void initRedis(PluginConfig pluginConf){
		
		String addr= pluginConf.getParam("redis_link");
		String addrs[] = addr.split("::");
		
		int port_num = Integer.valueOf(addrs[1]).intValue();
		String ip_addr = addrs[0];
		
		m_redis = new Jedis(ip_addr, port_num);
		System.out.println("spout's MsgPretreat init search redis success!");
	}
	
	private void init_output_Redis(PluginConfig pluginConf){
		
		String addr= pluginConf.getParam("redis_out_link");
		String addrs[] = addr.split("::");
		
		int port_num = Integer.valueOf(addrs[1]).intValue();
		String ip_addr = addrs[0];
		
		m_out_redis = new Jedis(ip_addr, port_num);
		System.out.println("spout's MsgPretreat init output redis success!");
	}
	
	//测试redis能否连接
	/*private void redistest(){
		//m_redis.set("tk1", "tangkun1");
		
		DataRecord record = m_loader.getRecord();
		if(record == null){
			return;
		}
		String word = "His_" + record.getField("word").getField().toString();
		
		m_redis.set("tk3", word);
	}*/
	
	/*private void log(){
		long currentTime = System.currentTimeMillis();
		if(currentTime > currentTime + m_interval){
			
		}
	}*/
	
	private void runpretreat() throws UnsupportedEncodingException{
//		m_logger.debug("spout's MsgPretreat begin : get info");//test
		
		DataRecord record = m_loader.getRecord();
		if(record == null){
			//m_logger.info("MsgPretreat no info");//test
			
			return;
		}
		String userlist = DataList.dm_user_list_set;
		
		String user = record.getField("user").getField();
		
		String word = record.getField("word").getField();

		//删除旧结果
		m_out_redis.del(m_appConf.getParam("redis_out_key_prefix") + ":" + user);
		
//		m_logger.debug("spout's MsgPretreat get user : "+ user + " info : " + word);
		
		//todo,检查推荐用户列表，该用户是否存在
		if(m_redis.sismember(userlist,user)){//正式改为用set存，key为user_list
			this.m_collector.emit(new Values(user,word));
			m_emitCnt++;
			
//			m_logger.debug("spout's MsgPretreat user : " + user + " info emit");
		}
		else {
//			m_logger.debug("spout's MsgPretreat user : "+ user + " is not in user_list ");
		}
		
		long currentTime = System.currentTimeMillis();
		if(currentTime > currentTime + m_interval){
//			m_logger.info(m_name+"has receive "+m_receiveCnt+
//					" tuples, and emit "+m_emitCnt+" tuples.");
			m_receiveCnt = 0;
			m_emitCnt = 0;
		}
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("spout's MsgPretreat begin : " + m_lastTime);
		
		m_receiveCnt++;
		
		try {
			runpretreat();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("spout's MsgPretreat end : " + m_lastTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("user","word"));
	}

}
