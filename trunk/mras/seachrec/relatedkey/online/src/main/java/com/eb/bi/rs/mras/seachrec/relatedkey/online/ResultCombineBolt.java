package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;


import org.apache.log4j.Logger;

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

public class ResultCombineBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private String boltName;
	//private Logger logger;
	
	private int tagRecNum;
	private int keyWordRecNum;
	private int totalRecNum;
	
	HashMap<String, HashMap<Integer, TreeSet<StringDoublePair>>> recResultMap = new HashMap<String, HashMap<Integer,TreeSet<StringDoublePair>>>();
	
	public ResultCombineBolt(String boltName) {
		this.boltName = boltName;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		LogUtil.getInstance().init(context.getThisComponentId(), String.valueOf(context.getThisTaskId()), boltName);
//		logger = LogUtil.getInstance().getLogger();
		
		
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());	
		System.out.println("bolt ResultCombineBolt prepare success!");
		
		//########应该从配置中读取，测试用##########
		tagRecNum = 4;
		keyWordRecNum = 4;
		totalRecNum = 4;
	
		
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long time = System.currentTimeMillis();
		System.out.println("bolt ResultCombineBolt execute begin : " + time);
		
		//++receiveCnt;
		
		String user= input.getStringByField("user");
		String word = input.getStringByField("word");
		
		TreeSet<StringDoublePair> associatedWordWeightSet = null;
		int flag = -1;
		if(input.getSourceComponent().equals("KeyWordRecBolt")){			
			associatedWordWeightSet = compute(input.getStringByField("keyWords"), input.getStringByField("weights"), keyWordRecNum);
			flag = 1;
		}else if(input.getSourceComponent().equals("TagRecBolt")){		
			associatedWordWeightSet = compute(input.getStringByField("tags"), input.getStringByField("weights"), tagRecNum) ;
			flag = 2;			
		}
		

		
		System.out.println(input.getMessageId());//test
		System.out.println(input.getMessageId().getAnchors());//test
		String key = user + "|" + word + "|" + input.getMessageId().getAnchors();
		if(recResultMap.containsKey(key)){
			HashMap<Integer, TreeSet<StringDoublePair>> associatedWordWeightMap = recResultMap.get(key);
			
			assert !associatedWordWeightMap.containsKey(flag);			
			TreeSet<StringDoublePair> keyWordWeightSet = null;
			TreeSet<StringDoublePair> tagWeightSet = null;
			
			if(flag == 1){
				keyWordWeightSet = associatedWordWeightSet;
				tagWeightSet = associatedWordWeightMap.get(2);				
			}else if(flag == 2){
				keyWordWeightSet = associatedWordWeightMap.get(1);
				tagWeightSet = associatedWordWeightSet;				
			}
			recResultMap.remove(key);//记得删除
			//结果集
			HashSet<String> associatedWordSet = new HashSet<String>();		
			
			while( associatedWordSet.size() < totalRecNum && (keyWordWeightSet.size() != 0 || tagWeightSet.size() != 0)){
				StringDoublePair keyWordWeight = keyWordWeightSet.first();
				if(keyWordWeight != null){
					associatedWordSet.add(keyWordWeight.getFirst());
					keyWordWeightSet.remove(keyWordWeight);	
				}	
				StringDoublePair tagWeight = tagWeightSet.first();
				if(tagWeight != null){
					associatedWordSet.add(tagWeight.getFirst());							
					tagWeightSet.remove(tagWeight);							
				}				
			}
			
			collector.emit(new Values(user, word, associatedWordSet));
			
		}else {
			HashMap<Integer, TreeSet<StringDoublePair>> associatedWordWeightMap = new HashMap<Integer, TreeSet<StringDoublePair>>();
			associatedWordWeightMap.put(flag, associatedWordWeightSet);
			recResultMap.put(key, associatedWordWeightMap);
		}		

		time = System.currentTimeMillis();
		System.out.println("bolt ResultCombineBolt execute end : " + time);

	}

	private TreeSet<StringDoublePair> compute(String words, String weights, int recNum) {
		String[] wordsArray = words.split(",");
		String[] weightsArray = weights.split(",");
		
		TreeSet<StringDoublePair> wordWeightSet =  new TreeSet<StringDoublePair>();
		if(wordsArray.length == weightsArray.length){			
			for(int i = 0; i < wordsArray.length; ++i){				
				StringDoublePair pair = new StringDoublePair(wordsArray[i],Double.parseDouble(weightsArray[i]));
				wordWeightSet.add(pair);
				if(wordWeightSet.size() > recNum){
					wordWeightSet.remove(wordWeightSet.last());
				}				
			}				
		}else{
			//logger.error("data error: word = " + words + ", weights = " + weights);	
			System.err.println("data error: word = " + words + ", weights = " + weights);
		}
		return wordWeightSet;
	}
	


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user","word","associatedWordSet"));		
	}
	
//	
//	@Override
//	public void cleanup() {
//		super.cleanup();
//	}

}
