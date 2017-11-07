package com.eb.bi.rs.opusstorm.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import redis.clients.jedis.Jedis;

import com.eb.bi.rs.frame2.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;
import com.eb.bi.rs.opusstorm.entity.OpusInfo;
import com.eb.bi.rs.opusstorm.util.Constants;
import com.eb.bi.rs.opusstorm.util.OrderUtils;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class HandleResultBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private HTableInterface opusInfoTable;
	private HTableInterface userPredictScoreTable;
	private HTableInterface userPredScoreCartoonTable;
	private HTableInterface userPredScoreComicTable;
	private HTableInterface userTagScoreTable;
	private HTableInterface userHistoryTable;
	private HTableInterface editScoreTable;

	private Jedis respJedis;
	private String respKey;
	private int respExpireTime;

	private String cartoon = null;
	private String comic = null;
	// 定时器
	private transient Thread loader = null;

	private Map<String, OpusInfo> opusInfoMap = new ConcurrentHashMap<String, OpusInfo>();
	private Map<String, Float> editScoreMap = new ConcurrentHashMap<String, Float>();
	private Map<String, Vector<String>> typeOpusMap = new ConcurrentHashMap<String, Vector<String>>();
	private Vector<String> lowQualityOpusSet = new Vector<String>();
	private Vector<String> rankSet = new Vector<String>();

	private float predictScorePer = 0;
	private float editScorePer = 0;

	private int topN = 0;

	private long readOpusInterval = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		System.out
				.println("--------------------HandleResultBolt prepare begin --------------------");
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());
		respKey = appConfig.getParam("response_table");
		respExpireTime = Integer.parseInt(appConfig
				.getParam("resp_expire_time"));
		String addrs[] = appConfig.getParam("resp_redis").split("::");
		respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		while (respJedis == null) {
			respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		}

		predictScorePer = Float.parseFloat(appConfig
				.getParam("predict_score_per"));
		editScorePer = Float.parseFloat(appConfig.getParam("edit_score_per"));

		topN = Integer.parseInt(appConfig.getParam("topN"));
		
		cartoon = appConfig.getParam("cartoon");
		comic = appConfig.getParam("comic");

		String[] rankStr = appConfig.getParam("ranks").split(",");
		for (String rank : rankStr) {
			rankSet.add(rank);
		}

		readOpusInterval = Long.parseLong(appConfig
				.getParam("read_opus_info_interval"));

		// 初始化Hbase配置
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				appConfig.getParam("hbase.zookeeper.quorum"));
		config.set("hbase.zookeeper.property.clientPort",
				appConfig.getParam("hbase.zookeeper.property.clientPort"));

		System.setProperty("java.security.krb5.kdc",
				appConfig.getParam("java.security.krb5.kdc"));
		System.setProperty("java.security.krb5.realm", "NBDP.COM");

		config.set("hadoop.security.authentication", "kerberos");
		config.set("hbase.security.authorization", "true");
		config.set("hbase.master.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.regionserver.kerberos.principal",
				"hbase/_HOST@NBDP.COM");
		config.set("hbase.thrift.kerberos.principal", "hbase/_HOST@NBDP.COM");
		config.set("hbase.zookeeper.client.kerberos.principal",
				"zookeeper/_HOST@NBDP.COM");

		config.set("fs.hdfs.impl",
				"org.apache.hadoop.hdfs.DistributedFileSystem");

		UserGroupInformation.setConfiguration(config);
		UserGroupInformation userGroupInformation;
		try {
			userGroupInformation = UserGroupInformation
					.loginUserFromKeytabAndReturnUGI("eb@NBDP.COM",
							"/data/eb/recsys/eb.keytab");
			UserGroupInformation.setLoginUser(userGroupInformation);
		} catch (IOException e) {
			e.printStackTrace();
		}

		HConnection con = null;
		while (con == null) {
			try {
				con = HConnectionManager.createConnection(config);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 动漫信息表
		while (opusInfoTable == null) {
			try {
				opusInfoTable = con.getTable(appConfig
						.getParam("opus_opus_info"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 动漫预测评分表
		while (userPredictScoreTable == null) {
			try {
				userPredictScoreTable = con.getTable(appConfig
						.getParam("opus_user_predict_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 动画预测评分表
		while (userPredScoreCartoonTable == null) {
			try {
				userPredScoreCartoonTable = con.getTable(appConfig
						.getParam("opus_user_predict_score_cartoon"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	
		// 漫画预测评分表
		while (userPredScoreComicTable == null) {
			try {
				userPredScoreComicTable = con.getTable(appConfig
						.getParam("opus_user_predict_score_comic"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 动漫标签推荐评分表
		while (userTagScoreTable == null) {
			try {
				userTagScoreTable = con.getTable(appConfig
						.getParam("opus_user_tag_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 用户历史表
		while (userHistoryTable == null) {
			try {
				userHistoryTable = con.getTable(appConfig
						.getParam("opus_user_history"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 动漫编辑分表
		while (editScoreTable == null) {
			try {
				editScoreTable = con.getTable(appConfig
						.getParam("opus_edit_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// 先Load一遍数据
		loadDataFromDB();

		// 定时器，每天定时从HBase读取动漫基本信息和编辑分
		if (loader == null) {
			loader = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(readOpusInterval);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						loadDataFromDB();
					}

				}
			});
			loader.setDaemon(true);
			loader.start();
		}
		System.out
				.println("--------------------HandleResultBolt prepare end--------------------");
	}

	void loadDataFromDB()
	{
		opusInfoMap.clear();
		editScoreMap.clear();
		typeOpusMap.clear();
		Scan scan = null;
		ResultScanner rs = null;
		try {
			scan = new Scan();
			scan.setCaching(100);
			/*
			 * long maxStamp = System.currentTimeMillis(); long minStamp =
			 * maxStamp - 24 * 60 * 60 * 1000; scan.setTimeRange(minStamp,
			 * maxStamp);
			 */
			rs = opusInfoTable.getScanner(scan);

			for (Result result : rs) {
				String opusId = Bytes.toString(result.getRow());
				String type = Bytes.toString(result.getValue(Constants.REPOCF,
						Constants.REPOCQTYPE));
				String rank = Bytes.toString(result.getValue(Constants.REPOCF,
						Constants.REPOCQRANK));
				if ((opusId != null && !"".equals(opusId))
						&& (type != null && !"".equals(type))) {
					// rank not in (a,s)
					if (!rankSet.contains(rank)) {
						lowQualityOpusSet.add(opusId);
						continue;
					}

					opusInfoMap.put(opusId, new OpusInfo(opusId, type, rank));
					if (typeOpusMap.containsKey(type)) {
						Vector<String> opusList = typeOpusMap.get(type);
						opusList.add(opusId);
						typeOpusMap.put(type, opusList);
					} else {
						Vector<String> opusList = new Vector<String>();
						opusList.add(opusId);
						typeOpusMap.put(type, opusList);
					}
				}
			}

			PrintHelper.print("opusInfoMap.size() : " + opusInfoMap.size());

			PrintHelper.print("lowQualityOpusSet.size() : "
					+ lowQualityOpusSet.size());

			PrintHelper.print("Type cartoon(1) of typeOpusMap : "
					+ typeOpusMap.get(cartoon).size());
			PrintHelper.print("Type comic(2) of typeOpusMap : "
					+ typeOpusMap.get(comic).size());
		} catch (Exception e) {
			PrintHelper.print("Get Opus Info From Hbase Error...");
			e.printStackTrace();
			return;
		}

		try {
			scan = new Scan();
			scan.setCaching(100);
			rs = editScoreTable.getScanner(scan);
			for (Result result : rs) {
				String opusId = Bytes.toString(result.getRow());
				String scoreStr = Bytes.toString(result.getValue(
						Constants.REPOCF, Constants.REPOCQEDITSCORE));
				float score = Float.parseFloat(scoreStr);
				editScoreMap.put(opusId, score);
			}
			PrintHelper.print("editScoreMap.size() : " + editScoreMap.size());
		} catch (Exception e) {
			PrintHelper.print("Get Opus Edit Score From Hbase Error...");
			e.printStackTrace();
		}
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long begin = System.currentTimeMillis();
		Map<String, Float> opusScoreMap = new HashMap<String, Float>();
		String userid = input.getStringByField("user");
		String type = input.getStringByField("type");
		PrintHelper.print("HandleResultBolt Receive userid : " + userid
				+ ", type : " + type);

		Map<String, Float> predictScoreMap = getUserItemcfScore(userid, type);
		PrintHelper.print("HandleResultBolt Predict Score Map 's Size : "
				+ predictScoreMap.size());

		// 预测评分、标签推荐评分与编辑分加权计算
		opusScoreMap = mixtureCaculate(userid, predictScoreMap);
		PrintHelper
				.print("HandleResultBoltAfter Mixture Caculate opusScoreMap's Size : "
						+ opusScoreMap.size());

		// 过滤
		Set<String> filterOpusSet = filterOpus(userid, type, opusScoreMap);
		PrintHelper
				.print("HandleResultBolt After Filter opusScoreMap's Size : "
						+ opusScoreMap.size());

		// 排序并取topN
		opusScoreMap = OrderUtils.sortByValue(opusScoreMap, topN);

		// 补白
		if (opusScoreMap.size() < topN) {
			filler(opusScoreMap, filterOpusSet, type);
		}
		PrintHelper.print("HandleResultBolt  user: " + userid
				+ " after filler:" + opusScoreMap);

		StringBuffer sb = new StringBuffer();
		Set<String> selectBook = opusScoreMap.keySet();
		for (String book : selectBook) {
			sb.append(book + "," + opusScoreMap.get(book) + "|");
		}
		if (selectBook.size() > 1) {
			sb.deleteCharAt(sb.length() - 1);
		}
		String returnCode = respJedis.setex(
				respKey + ":" + userid + ":" + type, respExpireTime,
				sb.toString());
		PrintHelper.print("HandleResultBolt user: " + userid
				+ " after save to redis return :" + returnCode);

		long end = System.currentTimeMillis();
		System.out.println("**************************Execute total costs: + "
				+ (end - begin) + "**************************");

	}

	/*
	 * 补白
	 */
	private void filler(Map<String, Float> opusScoreMap,
			Set<String> filterOpusSet, String type) {
		Vector<String> typeSet = typeOpusMap.get(type);
		if (typeSet == null || typeSet.isEmpty()) {
			return;
		}
		Vector<String> fillerSet = new Vector<String>(typeSet);
		
		// 先过滤不合格的动漫
		Iterator<String> iterator = fillerSet.iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (filterOpusSet.contains(key)) {
				iterator.remove();
			}
		}
		
		int fillerSize = fillerSet.size();
		int mapSize = opusScoreMap.size();
		int index = 0;
		if (fillerSize > 0 && fillerSize + mapSize > topN) {
			while (opusScoreMap.size() < topN) {
				try {
					Random random = new Random();
					index = random.nextInt(fillerSize);
					if (index < fillerSize) {
						String book = fillerSet.get(index);
						if (!opusScoreMap.containsKey(book)) {
							opusScoreMap.put(book, (float) -1);
						}
					}
				} catch (Exception e) {
					PrintHelper
							.print("Get book from filler vector out of range...");
					return;
				}
			}
		} else if (fillerSize + mapSize <= topN) {
			for (int i = 0; i < fillerSet.size(); i++) {
				String book = fillerSet.get(i);
				if (!opusScoreMap.containsKey(book)) {
					opusScoreMap.put(book, (float) -1);
				}
			}
		}
	}

	/*
	 * 混合计算动漫标签推荐得分，预测得分，编辑分
	 */
	private Map<String, Float> mixtureCaculate(String userId,
			Map<String, Float> predictScoreMap) {
		Map<String, Float> opusScoreMap = new HashMap<String, Float>();
		// 计算预测分Map中所有动漫的混合分
		Set<String> predictScoreSet = predictScoreMap.keySet();
		Iterator<String> it = predictScoreSet.iterator();
		float tempScore = 0;
		while (it.hasNext()) {
			String opus = it.next();
			tempScore = predictScoreMap.get(opus) * predictScorePer;
			if (opusInfoMap.containsKey(opus)) { // 不在推荐库中的动漫过滤掉
				if (editScoreMap.containsKey(opus)) {
					tempScore += editScoreMap.get(opus) * editScorePer;
				}
				opusScoreMap.put(opus, tempScore);
			}
			tempScore = 0;
		}
		return opusScoreMap;
	}

	/*
	 * 过滤用户历史，动画/漫画，低质量结果
	 */
	private Set<String> filterOpus(String userid, String type,
			Map<String, Float> opusScoreMap) {
		Set<String> filterOpusSet = new HashSet<String>();

		Iterator<Map.Entry<String, Float>> iterator = opusScoreMap.entrySet()
				.iterator();
		Vector<String> typeOpusSet = typeOpusMap.get(type);
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			String opusId = entry.getKey();
			//if (lowQualityOpusSet.contains(opusId)) { // 过滤低质量动漫
			if (!opusInfoMap.containsKey(opusId)) { // 过滤低质量动漫
				iterator.remove();
				continue;
			}
			if (!typeOpusSet.contains(opusId)) { // 过滤动画或漫画
				iterator.remove();
			}
		}
		return filterOpusSet;
	}

	/**
	 * 从Hbase表中查用户的预测得分
	 */
	public Map<String, Float> getUserItemcfScore(String userid, String type) {
		Map<String, Float> predictScoreMap = new HashMap<String, Float>();
		Get get = new Get(Bytes.toBytes(userid));
		Result result = null;
		try {
			if(cartoon.equals(type)){	// 动画
				result = userPredScoreCartoonTable.get(get);
			}else {					// 漫画
				result = userPredScoreComicTable.get(get);
			}
			String opusScores = Bytes.toString(result.getValue(
					Constants.REPOCF, Constants.REPOCQPRESCORE));
			if (opusScores != null) {
				String[] opusScore = opusScores.split("\\|");
				for (int i = 0; i < opusScore.length; i++) {
					String str = opusScore[i];
					String[] tmp = str.split(",");
					if (tmp.length == 2) {
						float preScore = Float.parseFloat(tmp[1]);
						predictScoreMap.put(tmp[0], preScore);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return predictScoreMap;
	}

	/**
	 * 从Hbase表中查用户的标签推荐得分
	 */
	public Map<String, Float> getUserTagScore(String userid) {
		Map<String, Float> tagScoreMap = new HashMap<String, Float>();
		Get get = new Get(Bytes.toBytes(userid));
		Result result = null;
		try {
			result = userTagScoreTable.get(get);
			String opusScores = Bytes.toString(result.getValue(
					Constants.REPOCF, Constants.REPOCQTAGSCORE));
			if (opusScores != null) {
				String[] opusScore = opusScores.split("\\|");
				for (int i = 0; i < opusScore.length; i++) {
					String str = opusScore[i];
					String[] tmp = str.split(",");
					if (tmp.length == 2) {
						float preScore = Float.parseFloat(tmp[1]);
						tagScoreMap.put(tmp[0], preScore);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tagScoreMap;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
