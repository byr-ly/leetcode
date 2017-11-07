package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.frame.common.storm.logutil.LogUtil;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode;
import com.huaban.analysis.jieba.SegToken;


/*
	通常情况下，实现一个Bolt，可以实现IRichBolt接口或继承BaseRichBolt，
	如果不想自己处理结果反馈，可以实现IBasicBolt接口或继承BaseBasicBolt，它实际上相当于自动做掉了prepare方法和collector.emit.ack(inputTuple)；
*/

public class KeyWordRecBolt  extends BaseBasicBolt{	
	
	private static final long serialVersionUID = 1L;
	private String boltName;
	//private Logger logger;
	private Jedis inputRedis;
	
	public KeyWordRecBolt(String boltName) {
		this.boltName = boltName;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		LogUtil.getInstance().init(context.getThisComponentId(), String.valueOf(context.getThisTaskId()), boltName);
//		logger = LogUtil.getInstance().getLogger();
		
		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
		initInputRedis(appConf);
		System.out.println("bolt KeyWordRecBolt prepare success!");
	}
	
	private void initInputRedis(PluginConfig pluginConf){		
		String addrs[] = pluginConf.getParam("redis_link").split("::");		
		inputRedis = new Jedis(addrs[0],Integer.parseInt(addrs[1]));
		System.out.println("bolt KeyWordRecBolt init input redis success!");
	}
	
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		long time = System.currentTimeMillis();
		System.out.println("bolt KeyWordRecBolt execute begin:" + time);
		
		//++receiveCnt;
		String user = input.getStringByField("user");
		String word = input.getStringByField("word");
		System.out.println("bolt KeyWordRecBolt receive tuple: "+ user + "|" + word);//test
		
		Map<String, String> keyWordWeightMap = new HashMap<String, String>();
		JiebaSegmenter segmenter = new JiebaSegmenter();			
		List<SegToken> tokens = segmenter.process(word, SegMode.SEARCH);
		
		for(SegToken token: tokens) {
			String srcKeyWord = token.token;
			Map<String, String> partkeyWordWeightMap = inputRedis.hgetAll(DataList.DM_KEY_RELATED_WEIGHT_HASH.getName() + ":" + srcKeyWord);
			System.out.println("bolt KeyWordRecBolt: source keyword is : " + srcKeyWord);
			
			if(partkeyWordWeightMap.isEmpty() )
				continue;				
			Iterator<Entry<String, String>> iterator = partkeyWordWeightMap.entrySet().iterator();
			while(iterator.hasNext()){					
	            Map.Entry<String, String> entry = iterator.next();
				String dstKeyWord = entry.getKey();
				String weight = entry.getValue();
			    System.out.println("bolt KeyWordRecBolt: destination keyword : "+ dstKeyWord + " ,weight : " + weight);		            		            
			    if(keyWordWeightMap.containsKey(dstKeyWord)){
	            	double a = Double.parseDouble(keyWordWeightMap.get(dstKeyWord));
	            	double b = Double.parseDouble(weight);
	            	keyWordWeightMap.put(dstKeyWord, String.valueOf(a + b));
	            }
	            else{
	            	keyWordWeightMap.put(dstKeyWord, weight);
	            }					
			}			
		}
		
		if(keyWordWeightMap.isEmpty()){
			System.out.println("bolt  KeyWordRecBolt : no keyword match!");//test			
			//###########为空也要发送吧，############
			//collector.emit(getClass().getSimpleName(), new Values(user, word, "", ""));
			collector.emit(new Values(user, word, "", ""));
			
		}
		else{		
			
			System.out.println(keyWordWeightMap.keySet().toString());//test
			System.out.println(keyWordWeightMap.values().toString());//test
			
			
			String keyWords = keyWordWeightMap.keySet().toString().substring(1, keyWordWeightMap.keySet().toString().length()-1);
			String weights = keyWordWeightMap.values().toString().substring(1, keyWordWeightMap.values().toString().length()-1);
			
			System.out.println("bolt KeyWordRecBolt : keyWords : " + keyWords);
			System.out.println("bolt KeyWordRecBolt : weights : " + weights);		
			
			//++emitCnt;
			//加上streamid,以便区分数据源，#########可以考虑不把容器转化为两个字符冲########
			//collector.emit(getClass().getSimpleName(), new Values(user, word, keyWords, weights));
			collector.emit(new Values(user, word, keyWords, weights));
		}			

		time = System.currentTimeMillis();
		System.out.println("bolt KeyWordRecBolt execute end : " + time);
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//用户|搜索词|关键词集|权重集
		declarer.declare(new Fields("user","word","keyWords","weights"));
	}
	

	
	
	

}
