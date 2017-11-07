package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

public class StormTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		PluginUtil.getInstance().init(args);
		//String app_confpath = PluginUtil.getInstance().getConfig().getConfigFilePath();//获取配置文件地址
		
//		Logger m_logger = PluginUtil.getInstance().getLogger();
		
//		m_logger.info("SearchRecomTopology open success! ");
		
		Config conf = new Config();
		
		//String appConf = appConfigStr(app_confpath);
//		String appConf = PluginUtil.getInstance().getConfig().getParam("redis_path", "10.1.70.4;8089");
		
//		conf.put("redis_path", appConf);
		
		conf.setNumWorkers(1);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		String spoutName = new String("input");
		builder.setSpout(spoutName, new SpoutTest(),1);
		String boltName1 = new String("weight");
		builder.setBolt(boltName1, new BoltTest(),1).shuffleGrouping(spoutName);
		
		try {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*本地模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(args[0], conf, builder.createTopology());
	    Utils.sleep(100000);
	    cluster.killTopology(args[0]);
	    cluster.shutdown();
	    */
		
		//任务已提交
//		m_logger.info("SearchRecomTopology put on storm !");
		//idox写入运行结果
//		PluginUtil.getInstance().getResult().save();
	}
/*
	public static String appConfigStr(String confpath) throws IOException{
		 String data = "";
		 String line = "";
		 
		 String path = confpath;
		 
		 BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
		 while((line = br.readLine()) != null ){
			 data += line;
		 }
		 
		 br.close();
		 
		 return data;
	}
*/
	

	public static class SpoutTest extends BaseRichSpout {

		/**
		 * 
		 */
		private static final long serialVersionUID = 2534525688069458991L;
		
		private int m_emitNumx = 0;
		private int m_emitNumy = 0;
		
		private SpoutOutputCollector m_collector;
		private TopologyContext m_context;
		//private Config m_conf;
		
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			//m_conf = (Config) conf;
			m_context = context;
			m_collector = collector;
		}
		
		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			if(m_emitNumy == 1000){
				return;
			}
			
			if(m_emitNumx == 1000){
				m_emitNumx=0;
				m_emitNumy++;
			}
			m_emitNumx++;
			
			this.m_collector.emit(new Values(m_emitNumy,m_emitNumx));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("emitNumX","emitNumY"));
		}
		
	}
	
	public static class BoltTest extends BaseBasicBolt {

		/**
		 * 
		 */
		private static final long serialVersionUID = -4155923476839154878L;
		private TopologyContext m_context;
		//private Config m_conf;
		
		private Jedis m_redis = null;
		
		@Override
		public void prepare(Map conf, TopologyContext context) {
			//m_conf = (Config) conf;
			m_context = context;
			
//			initRedis((String) m_conf.get("redis_path"));
			initRedis("10.1.70.4;8089");
		}
		
		private void initRedis(String redisPath){
			
			String addrs[] = redisPath.split(";");
			
			int port_num = Integer.valueOf(addrs[1]).intValue();
			String ip_addr = addrs[0];
			
			m_redis = new Jedis(ip_addr, port_num);
		}
		
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			if(input.size() == 0){
				return;
			}
			
			String NumX = String.valueOf(input.getIntegerByField("emitNumX").toString());
			String NumY = String.valueOf(input.getIntegerByField("emitNumY").toString());
			
			m_redis.sadd(NumX, NumY);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
