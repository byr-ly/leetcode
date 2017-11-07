package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
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
import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;



/*
 * 通常情况下（Shell和事务型的除外），实现一个Spout，可以直接实现接口IRichSpout
 * 如果不想写多余的代码，可以直接继承BaseRichSpout。
 * 
 * 没有BaseBasicSpout
 */
/*
 * 注意一些单例写的是否有问题
 */
public class MsgSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	private String spoutName;
	private SpoutOutputCollector collector;	
	//private Logger logger;
	private LoaderBase dataLoader;
	private Jedis inputRedis;
	private Jedis outputRedis; 
	private PluginConfig appConf;	
	private long counter;

	
	public MsgSpout(String spoutName){
		this.spoutName = spoutName;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {

		this.collector = collector;		
		
		//日志初始化
		//LogUtil.getInstance().init(context.getStormId(), String.valueOf(context.getThisTaskId()), spoutName);
		//logger = LogUtil.getInstance().getLogger();
	
		System.out.println("stromId = " + context.getStormId() + ",componentId =" + context.getThisComponentId()  + ",taskid = " + context.getThisTaskId());

		appConf = ConfigReader.getInstance().initConfig(conf.get("AppConfig").toString());	
		InputMsgManager.getInstance().init(appConf);
		dataLoader = InputMsgManager.getInstance().getLoader(spoutName);
		
		initInputRedis();
		initOutputRedis();		

		System.out.println("spout msgspout open success!");

	}

	private void initInputRedis(){
		
		String addrs[] = appConf.getParam("redis_link").split("::");		
		inputRedis = new Jedis(addrs[0],Integer.parseInt(addrs[1]));
		System.out.println("spout msgspout init input redis success!");
	}
	
	private void initOutputRedis(){
		
		String addrs[] = appConf.getParam("redis_out_link").split("::");		
		outputRedis = new Jedis(addrs[0], Integer.valueOf(addrs[1]).intValue());
		System.out.println("spout msgspout init output redis success!");
	}
	
	@Override
	public void nextTuple() {
		long time = System.currentTimeMillis();
		System.out.println("spout msgspout nextTuple begin : " + time);

		
		DataRecord record = dataLoader.getRecord();
		if(record == null){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		
		String user = record.getField("user").getField();
		String word = record.getField("word").getField();
		System.out.println("spout msgspout get user : "+ user + " searchWorld : " + word);
		//删除该用户的推荐结果
		outputRedis.del(appConf.getParam("redis_out_key_prefix") + ":" + user);
		
		//检查推荐用户列表，该用户是否存在
		if(inputRedis.sismember(DataList.DM_USER_LIST_SET.getName(), user)){
			this.collector.emit(new Values(user, word), ++counter);
			System.out.println("spout msgspout emit tuple[user: " + user + " ,search word: " + word + "]");
		}
		else {
			System.out.println("spout msgspout user : "+ user + " is not in user_list ");
		}
		
		time = System.currentTimeMillis();
		System.out.println("spout msgspout nextTuple end : " + time);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user","word"));
	}

}
