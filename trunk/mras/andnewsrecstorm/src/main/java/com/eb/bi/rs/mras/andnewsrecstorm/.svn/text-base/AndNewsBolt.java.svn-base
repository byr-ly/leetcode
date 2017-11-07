package com.eb.bi.rs.mras.andnewsrecstorm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class AndNewsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	static Logger log = Logger.getLogger(AndNewsBolt.class);

	private Producer<String, String> producer;
	private String returnTopic;
	private HTable participle_table; // 新闻分词表
	private HTable similarnews_table; // 相似新闻表
	private HTable keywordnews_table; // 关键字索引表
	private HTable src_andnews_table; // 源新闻数据表
	private HTable andnews_word_idf_table; // 词IDF数值表
	private int topN;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);

		// Kafka生产者配置
		PluginConfig appConf = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());
		String brokerList = appConf.getParam("broker_list");
		String zkConn = appConf.getParam("zkCfg");
		String groupId = appConf.getParam("group_id");
		returnTopic = appConf.getParam("returntopic");

		topN = Integer.parseInt(appConf.getParam("topN"));

		Properties props = new Properties();
		props.put("zookeeper.connect", zkConn);
		props.put("group.id", groupId);
		props.put("metadata.broker.list", brokerList);
		props.put("zookeeper.session.timeout.ms", "600");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		producer = new Producer<String, String>(new ProducerConfig(props));

		// HBase配置
		Configuration conf = HBaseConfiguration.create();
		String hbaseZk = appConf.getParam("hbaseZk");
		String hbaseZkPort = appConf.getParam("hbaseZkPort");
		conf.set("hbase.zookeeper.quorum", hbaseZk);
		conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort);

		try {
			participle_table = new HTable(conf,
					Bytes.toBytes("andnews_news_participle"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			similarnews_table = new HTable(conf,
					Bytes.toBytes("andnews_similar_news"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			keywordnews_table = new HTable(conf,
					Bytes.toBytes("andnews_keyword_news_nocategory"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			src_andnews_table = new HTable(conf,
					Bytes.toBytes("andnews_src_news"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			andnews_word_idf_table = new HTable(conf,
					Bytes.toBytes("andnews_word_idf"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("Receive>>>" + input.getString(0));
		char ch = 1;
		String line = "";
		try {
			line = splitJson(input.getString(0));
			if (line == null || line.equals("")) {
				return;
			}
		} catch (Exception e) {
			return;
		}
		String contid = null;
		String title = null;
		String classid = null;
		String content = null;

		String[] res = line.split(ch + "", -1);
		if (res.length == 4) {
			contid = res[0];
			title = res[1];
			classid = res[2];
			content = res[3];
		}

		if (contid != null && !"".equals(contid)) {

			if (classid == null || "".equals(classid)) {
				System.out.println("Newsid:" + contid
						+ "'s category is null, return...");
				return;
			}

			// 源新闻入Hbase表
			saveNews2Hbase(contid, classid, title, content);

			// 分词
			Map<String, NewsWord> newsWordMap = SegMore.segMore("N", title,
					content);
			String splitResult = splitWords(newsWordMap);
			System.out.println("[" + contid + "] " + "Split Result>>>"
					+ splitResult);
			if(splitResult == null & "".equals(splitResult)){
				return;
			}
			// 新闻分词结果存入新闻分词表
			saveSplit2Hbase(contid, classid, splitResult);

			String similarNews_retKafka = "";
			String similarNews_retHbase = "";
			Map<String, Float> similarMap = new HashMap<String, Float>();
			// 从关键字索引表中，查询News的分词结果对应的近期新闻和实时新闻及其权重，计算其相似度；
			similarMap = calSameClassKeysWeight(newsWordMap, splitResult,
					contid);

			Iterator<String> iterator = similarMap.keySet().iterator();
			while (iterator.hasNext()) {
				String news = iterator.next();
				similarNews_retKafka += news + "|";
				Float score = similarMap.get(news);
				similarNews_retHbase += news + "," + score + "|";
			}
			if (similarNews_retKafka.length() > 1) {
				similarNews_retKafka = similarNews_retKafka.substring(0,
						similarNews_retKafka.length() - 1);
			}

			// 相似新闻结果存入相似新闻表中
			if (!similarNews_retHbase.equals("")) {
				try {
					Put put = new Put(Bytes.toBytes(contid));
					put.add(Bytes.toBytes("cf"), Bytes.toBytes("similarnews"),
							Bytes.toBytes(similarNews_retHbase));
					similarnews_table.put(put);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			// 返回相似新闻推荐结果到Kafka集群
			JSONObject obj = new JSONObject();
			try {
				obj.accumulate("contid", contid);
				obj.accumulate("similar_news", similarNews_retKafka);
				obj.accumulate("reserve", "");
				System.out.println("[" + contid + "] " + "Return to AndNews>>>"
						+ obj.toString());
				String msg = obj.toString();
				producer.send(new KeyedMessage<String, String>(returnTopic, msg));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * 源新闻入Hbase表，rowkey:时间_id，cf:分类，cf:标题,cf:内容
	 */
	private void saveNews2Hbase(String contid, String classid, String title,
			String content) {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		String dateStr = sdf.format(date);
		try {
			Put put = new Put(Bytes.toBytes(dateStr + "_" + contid));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("category"),
					Bytes.toBytes(classid));
			src_andnews_table.put(put);

			put.add(Bytes.toBytes("cf"), Bytes.toBytes("title"),
					Bytes.toBytes(title));
			src_andnews_table.put(put);

			put.add(Bytes.toBytes("cf"), Bytes.toBytes("content"),
					Bytes.toBytes(content));
			src_andnews_table.put(put);
		} catch (Exception e) {
			System.out.println("save src news to hbase fail...");
			e.printStackTrace();
		}

	}

	/*
	 * 从关键字索引表中，查询同分类下的News1的分词结果对应的近期新闻和实时新闻及其权重，计算其相似度；
	 */
	private Map<String, Float> calSameClassKeysWeight(
			Map<String, NewsWord> newsWordMap, String splitResult, String contid) {
		Map<String, String> newsWeightMap = new HashMap<String, String>();
		Iterator<String> it = newsWordMap.keySet().iterator();
		while (it.hasNext()) {
			String keyWord = it.next();
			String rowKey = keyWord;
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = null;
			try {
				result = keywordnews_table.get(get);
				if (!result.isEmpty()) {
					String newsWeight = Bytes.toString(result.getValue(
							Bytes.toBytes("cf"), Bytes.toBytes("news"))); // 格式：k1:n1,p1|n2,p2|...
					String[] tmp = newsWeight.split("\\|", -1);
					if (tmp.length > 0) {
						for (String str : tmp) {
							String[] newsW = str.split(",", -1);
							if (newsW.length == 2) {
								String news = newsW[0];
								String weight = newsW[1];
								if (newsWeightMap.containsKey(news)) { // 转为：n1:k1,p1|k2,p2
									String val = newsWeightMap.get(news);
									newsWeightMap.put(news, val + "|" + keyWord
											+ "," + weight);
								} else {
									newsWeightMap.put(news, keyWord + ","
											+ weight);
								}
							}
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 新到新闻分词及其权重
		Map<String, String> newKeyWeightMap = new HashMap<String, String>();
		double vectorModule = 0;
		String[] newKeyWeight = splitResult.split("\\|", -1);
		for (String str : newKeyWeight) {
			String[] kW = str.split(",", -1);
			if (kW.length == 2) {
				String key = kW[0];
				String weight = kW[1];
				newKeyWeightMap.put(key, weight);
				try {
					double d = Double.parseDouble(weight);
					vectorModule += Math.pow(d, 2);
				} catch (Exception e) {
					continue;
				}
			}
		}
		vectorModule = Math.sqrt(vectorModule);

		// 计算新闻相似度
		Map<String, Float> similarMap = new HashMap<String, Float>();
		Iterator<String> itKey = newsWeightMap.keySet().iterator();
		while (itKey.hasNext()) {
			double mult = 0;
			double vectorModTmp = 0;
			String news = itKey.next();
			String keysWeights = newsWeightMap.get(news);
			String[] tmp = keysWeights.split("\\|", -1);
			if (tmp.length > 0) {
				for (String str : tmp) {
					String[] keysW = str.split(",", -1);
					if (keysW.length == 2) {
						String key = keysW[0];
						String weight = keysW[1];
						try {
							double w2 = Double.parseDouble(weight);
							vectorModTmp += Math.pow(w2, 2);
							if (newKeyWeightMap.containsKey(key)) {
								double w1 = Double.parseDouble(newKeyWeightMap
										.get(key));
								mult += w1 * w2;
							}
						} catch (Exception e) {
							continue;
						}
					}
				}
			}
			vectorModTmp = Math.sqrt(vectorModTmp);
			float module = (float) (vectorModule * vectorModTmp);
			if (module == 0) {
				continue;
			}
			float similarity = (float) (mult / module);
			if (!Float.isNaN(similarity) && !news.equals(contid)) {
				similarMap.put(news, similarity);
			}
		}
		System.out.println("[" + contid + "] "
				+ "Before topN similarMap's size : " + similarMap.size());
		similarMap = MergeUtils.sortMapByValue(similarMap, topN);
		System.out.println("[" + contid + "] " + "SimilarMap>>>" + similarMap);
		return similarMap;
	}

	/*
	 * 新闻分词结果存入新闻分词表
	 */
	private void saveSplit2Hbase(String contid, String classid,
			String splitResult) {
		try {
			Put put = new Put(Bytes.toBytes(contid));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("class"),
					Bytes.toBytes(classid));
			participle_table.put(put);
			put = new Put(Bytes.toBytes(contid));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("participle"),
					Bytes.toBytes(splitResult));
			participle_table.put(put);
		} catch (Exception e) {
			System.out.println("save split result to hbase fail...");
			e.printStackTrace();
		}

	}

	/*
	 * 对新闻标题和内容进行分词,返回结果k1,p1|k2,p2|...
	 */
	private String splitWords(Map<String, NewsWord> map) {
		int totalWordNums = 0;
		StringBuffer values = new StringBuffer("");
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String word = it.next();
			NewsWord newsWord = map.get(word);
			values.append(newsWord.word);
			values.append(",");
			values.append(newsWord.times);
			values.append("|");
			totalWordNums += newsWord.times;
		}

		DecimalFormat df = new DecimalFormat("#.0000");
		StringBuffer sb = new StringBuffer("");

		String[] allWords = values.toString().split("\\|", -1);
		for (String word : allWords) {
			String[] fields = word.split(",", -1);
			if (fields.length != 2) {
				continue;
			}
			String splitWord = fields[0];
			int times = Integer.parseInt(fields[1]);
			double tf = (double) times / totalWordNums;
			String wordIdf = "10";
			String rowKey = splitWord;
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = null;
			double weight = 0.0;
			try {
				result = andnews_word_idf_table.get(get);
				if (!result.isEmpty()) {
					wordIdf = Bytes.toString(result.getValue(
							Bytes.toBytes("cf"), Bytes.toBytes("idf")));
					double idf = Double.parseDouble(wordIdf);
					weight = tf * idf;
					sb.append(splitWord);
					sb.append(",");
					sb.append(df.format(weight));
					sb.append("|");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	/*
	 * 从Json中解析出新闻的ID，标题，分类，内容
	 */
	private String splitJson(String input) {
		String result = "";
		try {
			JSONObject inputJson = new JSONObject(input);
			Iterator<String> it = inputJson.keys();
			String contid = null;
			String title = null;
			String category = null;
			String content = null;
			String publish_status = ""; //新闻的发布状态，1：表示已发布，0,：表示下线，-1：表示新闻未发布
			while (it.hasNext()) {
				String key = it.next();
				if (key.equals("id")) {
					contid = inputJson.getString(key);
				} else if (key.equals("title")) {
					title = inputJson.getString(key);
				} else if (key.equals("category")) {
					category = inputJson.getString(key);
				} else if (key.equals("content")) {
					content = inputJson.getString(key);
				} else if (key.equals("publish_status")) {
					publish_status = inputJson.getString(key);
				}
			}
			
			if(!publish_status.equals("1")){	// 非已发布新闻都不处理
				return result;
			}

			StringBuilder sb = new StringBuilder();
			boolean skip = false;
			for (int i = 0; i < content.length(); i++) {
				char ch = content.charAt(i);
				if (ch == '<') {
					skip = true;
				} else if (ch == '>') {
					skip = false;
				} else {
					if (!skip) {
						sb.append(ch);
					}
				}
			}
			char ch = 1;
			title = title.replaceAll("[\\pP￥$^+~`|<>\r\n]", " ");
			String cont = sb.toString().replaceAll("[\\pP￥$^+~`|<>\r\n]", " ");
			result = contid + ch + title + ch + category + ch + cont;
			return result;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
