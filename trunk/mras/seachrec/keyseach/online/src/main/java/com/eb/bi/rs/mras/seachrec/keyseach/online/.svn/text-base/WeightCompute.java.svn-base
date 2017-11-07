package com.eb.bi.rs.mras.seachrec.keyseach.online;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;

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

public class WeightCompute extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6445550930364102732L;
	
	private String m_name;
	
//	private Logger m_logger;
	private TopologyContext m_context;
	private long m_lastTime;
	
	private Jedis m_redis = null;
	
	private int m_receiveCnt;
	private long m_emitCnt;

	public WeightCompute(String boltName) {
		// TODO Auto-generated constructor stub
		m_name = boltName;
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
		
		initRedis(pluginConf);
		
		System.out.println("bolt's WeightCompute prepare success!");
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's WeightCompute begin : " + m_lastTime);
		
		m_receiveCnt++;
		
		String user = input.getStringByField("user");
		String word = input.getStringByField("word");
		
//		m_logger.debug("bolt's WeightCompute get info success! "+ user + "|" + word);//test
		
		int type = -1;
		String [] tagstr = new String []{"",""};
		
		/*test
		if(tagstr != null){
			m_logger.info("not empty!");//test
		}
		*/
		
		if(m_redis.exists(DataList.dim_book_name_re_id_set + ":" + word)){
			type = 0;
			
			tagstr = getTags(word,type);
			
			//tagstr = getTags(m_redis.get(DataList.dim_book_name_re_id_string + ":" + word),type);
//			m_logger.debug("word is bookname");//test
		}
		else{
			if(m_redis.exists(DataList.dim_author_name_re_id_set + ":" + word)){
				type = 1;
				
				tagstr = getTags(word,type);
				
				//tagstr = getTags(m_redis.get(DataList.dim_author_name_re_id_string + ":" + word),type);
//				m_logger.debug("word is authorname");//test
			}
			else{
				type = 2;
				tagstr = getTags(word,type);
//				m_logger.debug("word is key");//test
			}
		}
		if(tagstr != null){
			m_emitCnt++;
//			m_logger.debug("keys are : "+tagstr[0].toString());//test
//			m_logger.debug("vals are : "+tagstr[1].toString());//test
			collector.emit(new Values(user,word,String.valueOf(type),tagstr[0],tagstr[1]));
		}
		else{
//			m_logger.debug("is empty!");//test
		}
		
		m_lastTime = System.currentTimeMillis();
//		m_logger.info("bolt's WeightCompute end : " + m_lastTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//用户|搜索词（是否写改为分词后拼接的）|搜索类型|标签集
		declarer.declare(new Fields("user","word","type","tags_key","tags_val"));
	}
	
	//查询初始化方式待定
	private void initRedis(PluginConfig pluginConf){
		String addr= pluginConf.getParam("redis_link");
		
		//m_logger.info("WeightCompute initRedis is" + addr);//test
		
		String addrs[] = addr.split("::");
		int port = Integer.valueOf(addrs[1]).intValue();
		String ip = addrs[0];
		m_redis = new Jedis(ip, port);
		System.out.println("bolt's WeightCompute init redis success!");
	}
	
	//标签权重匹配
	public String[] getTags(String word, int type){
//		m_logger.debug("bolt's WeightCompute : getTags is coming " + word + " " + type);//test
		
		String [] tags = new String []{"",""};//整合后结果
		Map<String, String> weight = new HashMap<String, String>();//结果存储
		//搜索类型判断
		if(type==0||type==1){
			
			//得到图书或作者id集合
			Set<String> IDs = new HashSet<String>();
			
			if(type==0){
				IDs = m_redis.smembers(DataList.dim_book_name_re_id_set + ":" + word);
//				m_logger.debug("bolt's WeightCompute : IDs is : " + IDs);//test
			}
			
			if(type==1){
				IDs = m_redis.smembers(DataList.dim_author_name_re_id_set + ":" + word);
//				m_logger.debug("bolt's WeightCompute : IDs is : " + IDs);//test
			}
			
			//关联标签权重计算
			Iterator<String> it0 = IDs.iterator();
			while(it0.hasNext()){
				Map<String, String> weight_tmp = new HashMap<String, String>();
				
				if(type==0)
					weight_tmp = m_redis.hgetAll(DataList.dm_bookid_tag_weight_hash + ":" + it0.next());
				if(type==1)
					weight_tmp = m_redis.hgetAll(DataList.dm_authorid_tag_weight_hash + ":" + it0.next());
				
				if(weight_tmp.isEmpty())
					continue;
				
				Set<Map.Entry<String, String>> set = weight_tmp.entrySet();
		        for (Iterator<Map.Entry<String, String>> it = set.iterator(); it.hasNext();) {
		            Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
		            
//		            m_logger.debug("bolt's WeightCompute : key : "+ entry.getKey() + " val : " + entry.getValue());//test
		            
		            if(weight.containsKey(entry.getKey())){
		            	double labelValue = 0.0;
		            	double a = Double.valueOf(weight.get(entry.getKey())).doubleValue();
			            double b = Double.valueOf(entry.getValue()).doubleValue();
		            	labelValue = 1.0-(1.0-a)*(1.0-b);
		            	weight.put(entry.getKey(), Double.toString(labelValue));
		            }
		            else{
		            	weight.put(entry.getKey(), entry.getValue());
//		            	if(!weight.isEmpty())//test
//		            	{
//		            		m_logger.debug(weight.toString());//test
//		            	}
		            }
		        }
			}
		}
		else{
			//分词
			JiebaSegmenter segmenter = new JiebaSegmenter();
			
			List<SegToken> tokens = segmenter.process(word, SegMode.SEARCH);
			
			List<String> words = new ArrayList<String>();
			
			for(SegToken token: tokens){
				String keyword = token.token;
				words.add(keyword);
			}
			
			//words = segmenter.sentenceProcess(word);
			
			for(int i = 0;i != words.size();i++){
				Map<String, String> weight_tmp = new HashMap<String, String>();
				weight_tmp = m_redis.hgetAll(DataList.dm_searchword_tag_weight_hash + ":" + words.get(i));
				
//				m_logger.debug("bolt's WeightCompute : key is : " + words.get(i) + " size is : " + words.size());//test
				
				if(weight_tmp.isEmpty())
					continue;
				
				Set<Map.Entry<String, String>> set = weight_tmp.entrySet();
		        for (Iterator<Map.Entry<String, String>> it = set.iterator(); it.hasNext();) {
		            Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
		            
//		            m_logger.debug("bolt's WeightCompute : key : "+ entry.getKey() + " val : " + entry.getValue());//test
		            
		            if(weight.containsKey(entry.getKey())){
		            	double labelValue = 0.0;
		            	double a = Double.valueOf(weight.get(entry.getKey())).doubleValue();
			            double b = Double.valueOf(entry.getValue()).doubleValue();
		            	labelValue = 1.0-(1.0-a)*(1.0-b);
		            	weight.put(entry.getKey(), Double.toString(labelValue));
		            }
		            else{
		            	weight.put(entry.getKey(), entry.getValue());
//		            	if(!weight.isEmpty())//test
//		            		m_logger.debug(weight.toString());//test
		            }
		        }
			}
		}
		
		//返回结果
		if(weight.isEmpty()){
//			m_logger.info("bolt's WeightCompute : no weight match result !");//test
			return null;
		}
		else{
			int key_Size = weight.keySet().toString().length()-1;
			int value_Size = weight.values().toString().length()-1;
			
//			m_logger.debug(weight.keySet().toString());//test
//			m_logger.debug(weight.values().toString());//test
			
//			m_logger.debug("bolt's WeightCompute : getTags size ! key's&val's size : "+ key_Size + " : " + value_Size);//test
			
			tags[0] = weight.keySet().toString().substring(1, key_Size);
			tags[1] = weight.values().toString().substring(1, value_Size);
			
//			m_logger.debug("bolt's WeightCompute : keys : " + tags[0]);//test
//			m_logger.debug("bolt's WeightCompute : vals : " + tags[1]);//test
			
			return tags;
		}
	}
	
	@Override
	public void cleanup() {
    
	}
}
