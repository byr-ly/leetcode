package com.eb.bi.rs.mras.seachrec.relatedkey.online;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

public class TagRecBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private String boltName;
	//private Logger logger;
	private Jedis inputRedis;

	public TagRecBolt(String boltName) {
		this.boltName = boltName;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//接口有问题
//		LogUtil.getInstance().init(context.getStormId(), String.valueOf(context.getThisTaskId()), boltName);
//		logger = LogUtil.getInstance().getLogger();		
		
		PluginConfig pluginConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());		
		initInputRedis(pluginConf);
		System.out.println("bolt TagRecBolt prepare success!");
	}
	
	private void initInputRedis(PluginConfig pluginConf){		
		String addrs[] = pluginConf.getParam("redis_link").split("::");		
		inputRedis = new Jedis(addrs[0],Integer.parseInt(addrs[1]));
		System.out.println("bolt TagRecBolt init input redis success!");
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long time = System.currentTimeMillis();
		System.out.println("bolt TagRecBolt execute begin : " + time);
		
		//++receiveCnt;
		String user = input.getStringByField("user");
		String word = input.getStringByField("word");	
		
		System.out.println("bolt TagRecBolt get info success! "+ user + "|" + word);//test
		
		int type = -1;		
		String[] tagWeightStr = new String[2];
		if(inputRedis.exists(DataList.DIM_BOOK_NAME_RE_ID_SET.getName() + ":" + word)) {//搜索的是图书名
			type = 0;			
			System.out.println("search word is book name");
		}
		else if(inputRedis.exists(DataList.DIM_AUTHOR_NAME_RE_ID_SET.getName() + ":" + word)){//搜索的作者名
			type = 1;
			System.out.println("search word is author name");			
		}else{
			type = 2;
			System.out.println("search word is neither a book name nor a author name ");//既不是图书名也不是作业名			
		}
		tagWeightStr = getTagWeights(word, type);	
		
		if(tagWeightStr != null){
			//++emitCnt;
			System.out.println("tags are : "+ tagWeightStr[0]);
			System.out.println("weights are : "+ tagWeightStr[1]);
			//加上streamid，以便区分数据来源
			//collector.emit(getClass().getSimpleName(),new Values(user, word, tagWeightStr[0], tagWeightStr[1]));
			collector.emit(new Values(user, word, tagWeightStr[0], tagWeightStr[1]));
		}
		else{
			System.out.println("is empty!");//test
			//就算是空，也得发送
			collector.emit(new Values(user, word, "", ""));
		}
		
		time = System.currentTimeMillis();
		System.out.println("bolt's TagRecBolt execute end : " + time);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//用户|搜索词|搜索类型|标签集|权重集
		declarer.declare(new Fields("user","word","tags","weights"));
	}
	

	//标签权重匹配
	private String[] getTagWeights(String word, int type){
		System.out.println("bolt TagRecBolt : getTagWeights is called " + word + " " + type);		
		String[] tagWeightStr = new String[2];
		Map<String, String> tagWeightMap = new HashMap<String, String>();//最终结果存储
		if( type == 0 || type == 1){
			Set<String> ids = null;			
			if(type == 0){//搜索的是图书名
				ids = inputRedis.smembers(DataList.DIM_BOOK_NAME_RE_ID_SET.getName()+ ":" + word);
				System.out.println("bolt TagRecBolt : book IDS  : " + ids);
			}else{//搜索的是作者名
				ids = inputRedis.smembers(DataList.DIM_AUTHOR_NAME_RE_ID_SET.getName() + ":" + word);
				System.out.println("bolt TagRecBolt : author IDs : " + ids);
			}
			
			//关联标签权重计算
			Iterator<String> idIter = ids.iterator();
			while(idIter.hasNext()){
				Map<String, String> partTagWeightMap = null;				
				if(type == 0){
					partTagWeightMap = inputRedis.hgetAll(DataList.DM_BOOKID_TAG_WEIGHT_HASH.getName() + ":" + idIter.next());
				}
				else if(type == 1){
					partTagWeightMap = inputRedis.hgetAll(DataList.DM_AUTHORID_TAG_WEIGHT_HASH.getName() + ":" + idIter.next());
				}
				
				if(partTagWeightMap.isEmpty())
					continue;	
				
				Iterator<Entry<String, String>> iterator = partTagWeightMap.entrySet().iterator();
				while(iterator.hasNext()){
					Entry<String, String> entry = iterator.next();	
					String tag = entry.getKey();
					String weight = entry.getValue();
				    System.out.println("bolt  TagRecBolt : tag : "+ tag + " weight : " + weight);			            
		            if(tagWeightMap.containsKey(tag)){
		            	double a = Double.parseDouble(tagWeightMap.get(tag));
		            	double b = Double.parseDouble(weight);
		            	tagWeightMap.put(tag, String.valueOf(a + b));
		            }
		            else{
		            	tagWeightMap.put(tag, weight);
		            }					
				}
			}
		}
		else{			
			//分词			
			JiebaSegmenter segmenter = new JiebaSegmenter();			
			List<SegToken> tokens = segmenter.process(word, SegMode.SEARCH);
			
			for(SegToken token: tokens) {
				String keyword = token.token;	
				Map<String, String> partTagWeightMap = inputRedis.hgetAll(DataList.DM_SEARCHWORD_TAG_WEIGHT_HASH.getName() + ":" + keyword);
				System.out.println("bolt TagRecBolt : keyword is : " + keyword);
				
				if(partTagWeightMap.isEmpty())
					continue;				
				Iterator<Entry<String, String>> iterator = partTagWeightMap.entrySet().iterator();
				while(iterator.hasNext()){					
		            Map.Entry<String, String> entry = iterator.next();
					String tag = entry.getKey();
					String weight = entry.getValue();
				    System.out.println("bolt TagRecBolt : tag : "+ tag + " weight : " + weight);		            		            
				    if(tagWeightMap.containsKey(tag)){
		            	double a = Double.parseDouble(tagWeightMap.get(tag));
		            	double b = Double.parseDouble(weight);
		            	tagWeightMap.put(tag, String.valueOf(a + b));
		            }
		            else{
		            	tagWeightMap.put(tag, weight);
		            }					
				}			
			}
		}
		//返回结果
		if(tagWeightMap.isEmpty()){
			System.out.println("bolt TagRecBolt: no tag match!");//test
			return null;
		}
		else{		
			
			System.out.println(tagWeightMap.keySet().toString());//test
			System.out.println(tagWeightMap.values().toString());//test
			
			
			tagWeightStr[0] = tagWeightMap.keySet().toString().substring(1, tagWeightMap.keySet().toString().length()-1);
			tagWeightStr[1] = tagWeightMap.values().toString().substring(1, tagWeightMap.values().toString().length()-1);
			
			System.out.println("bolt TagRecBolt : tags : " + tagWeightStr[0]);
			System.out.println("bolt TagRecBolt : weights : " + tagWeightStr[1]);
			
			return tagWeightStr;

		}
	}
	
	@Override
	public void cleanup() {
    
	}
}
