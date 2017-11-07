package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;

public class SaveResultBolt extends BaseBasicBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String boltName;
	//private Logger logger;
	private PluginConfig appConf;
	private Jedis outputRedis;
	private HTable hbaseTable;
	
	
	
	
	public SaveResultBolt(String boltName) {
		this.boltName = boltName;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		LogUtil.getInstance().init(context.getThisComponentId(), String.valueOf(context.getThisTaskId()), boltName);
//		logger = LogUtil.getInstance().getLogger();
		
		
		appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
		initOutputRedis();
		initHbase();
		System.out.println("bolt SaveResultBolt prepare success!");
	
	}
	
	private void initOutputRedis(){
		
		String addrs[] = appConf.getParam("redis_out_link").split("::");		
		outputRedis = new Jedis(addrs[0], Integer.valueOf(addrs[1]).intValue());
		System.out.println("bolt SaveResultBolt  init output redis success!");
	}

	private void initHbase() {
		Configuration hConf = HBaseConfiguration.create();
		//这不应该在配置里面吗？
		hConf.set("hbase.zookeeper.quorum", "eb170,eb171,eb174");
		hConf.set("hbase.zookeeper.property.clientPort", "2181");		
		
		try {
			hbaseTable = new HTable(hConf,  appConf.getParam("result_table"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("bolt SaveResultBolt init Hbase ok! ");
	}
	

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long time = System.currentTimeMillis();
		System.out.println("bolt ResultCombineBolt execute begin : " + time);
		String user = input.getString(0);
		@SuppressWarnings("unchecked")
		HashSet<String> associatedWordSet = (HashSet<String>)input.getValue(2);
		
		JSONArray result = new JSONArray();
		
		Iterator<String> iterator = associatedWordSet.iterator();
		while(iterator.hasNext()){
			result.put(iterator.next());			
		}
		//确认下在redis里面的存储结构
		//outputRedis.set(appConf.getParam("redis_out_key_prefix") + ":" + user, );//test
		outputRedis.lpush(appConf.getParam("redis_out_key_prefix") + ":" + user, result.toString());
		
		Put put = new Put(Bytes.toBytes(user));  
		put.add(Bytes.toBytes("result"), Bytes.toBytes(""), Bytes.toBytes(result.toString()));
		try {
			hbaseTable.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		time = System.currentTimeMillis();
		System.out.println("bolt's SaveResultBolt end : " + time);			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
