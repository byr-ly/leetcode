package com.eb.bi.rs.mras.seachrec.keyseach.online;

//import java.io.IOException;
import java.util.ArrayList;
//import java.util.List;
import java.util.Map;
/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
*/

import org.apache.log4j.Logger;
import org.json.JSONArray;

import redis.clients.jedis.Jedis;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;

//import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ResultProcess extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6678345823904053287L;
	private String m_name;
//	private Logger m_logger;
	private TopologyContext m_context;
	//private HTable m_table;
	private PluginConfig m_pluginConf;
	private long m_lastTime;
	private Jedis m_redis = null;
	
	private int m_number;
	
	public ResultProcess(String name){
		m_name = name;
	}
	
	public void prepare(Map conf, TopologyContext context){
		m_context = context;
		
//		LogUtil.getInstance().init(m_context.getStormId(), String.valueOf(m_context.getThisTaskId()), m_name);
//		m_logger = LogUtil.getInstance().getLogger();
		
		String appConfig = (String) conf.get("AppConfig");
		m_pluginConf = ConfigReader.getInstance().initConfig(appConfig);
		
		String number = m_pluginConf.getParam("rec_num");
		m_number = Integer.valueOf(number).intValue();
		
		initRedis(m_pluginConf);
		
		/*
		try {
			initHbase();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	/*
	private void initHbase() throws IOException {
		// TODO Auto-generated method stub
		Configuration hConf = HBaseConfiguration.create();
		hConf.set("hbase.zookeeper.quorum", "eb170,eb171,eb174");
		hConf.set("hbase.zookeeper.property.clientPort", "2181");
		String tableName = m_pluginConf.getParam("result_table");
		m_logger.debug("initHbase table! "+ tableName);
		m_table = new HTable(hConf, tableName);
		
		m_logger.info("initHbase ok! ");
	}
	*/
	
	private void initRedis(PluginConfig pluginConf) {
		// TODO Auto-generated method stub
		String addr= pluginConf.getParam("redis_out_link");
		
		//System.out.println("ResultProcess initRedis is : " + addr);//test
		
		String addrs[] = addr.split("::");
		int port = Integer.valueOf(addrs[1]).intValue();
		String ip = addrs[0];
		m_redis = new Jedis(ip, port);
		
		System.out.println("ResultProcess init redis success !");//test
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's ResultProcess begin : " + m_lastTime);
		
		if(input.size() == 0){
			return;
		}
		
		String rowkey = input.getStringByField("user");
		
		/*
		String value = input.getStringByField("book1")+"|"
					   +input.getStringByField("book2")+"|"
					   +input.getStringByField("book3");
		*/
		String rmdword = "";
		String value = new String(rowkey + "|" +rmdword);
		
		ArrayList<String> bookresult = new ArrayList<String>();
		
		bookresult = (ArrayList<String>) input.getValueByField("books");
		
//		m_logger.debug("bolt's ResultProcess get info , bookid is : " + bookresult.toString());//test
		
		JSONArray j_result = new JSONArray();
		
		//入库持久化拼接处理
		for(int i = bookresult.size(); i != 0; i--){
			value = value + "|" + bookresult.get(i-1);
			
			j_result.put(bookresult.get(i-1));
		}
		
		if(bookresult.size() < 3){
			for(int j=0; j != m_number-bookresult.size(); j++){
				value = value + "|";
			}
		}
		
		//反馈前台入redis处理
		m_redis.set(m_pluginConf.getParam("redis_out_key_prefix") + ":" + rowkey, j_result.toString());//test
		
//		m_logger.info(input.getMessageId()+" Put: "+rowkey+", "+ value);//test
		
		/*
		Put put = new Put(Bytes.toBytes(rowkey));  
		put.add(Bytes.toBytes("result"), Bytes.toBytes(""), Bytes.toBytes(value));
		try {
			m_table.put(put);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's ResultProcess end : " + m_lastTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
