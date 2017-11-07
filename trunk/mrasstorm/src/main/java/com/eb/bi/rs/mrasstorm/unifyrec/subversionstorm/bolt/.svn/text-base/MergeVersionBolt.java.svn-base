package com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.domain.BookInfo;
import com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.util.MergeUtils;
import com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.util.SlowLog;
import com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.util.TimeUtil;

/**
 * 用于处理推荐结果：混合，过滤，排序，补白
 * 
 * @author ynn
 * @date 创建时间：2015-10-20 上午11:00:50
 * @version 1.0
 */
public class MergeVersionBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private HTableInterface userPrefTable; // 用户偏好图书表
	private static final byte[] REPOCF = Bytes.toBytes("cf"); // hbase 列族
	private static final byte[] REPOCQSCORE = Bytes.toBytes("c"); // hbase 列名
	private HTableInterface malePrefTable; // 男生偏好图书表
	private HTableInterface femalePrefTable; // 女生偏好图书表
	private HTableInterface pubPrefTable; // 出版偏好图书表

	private HTableInterface userRealTable;// 实时图书行为表
	private HTableInterface userGreyBooksTable;// 用户灰名单表
	private HTableInterface userBlackBooksTable;// 用户黑名单表
	public static final byte[] REPOCQEXPIREDATE = Bytes.toBytes("expiredate");

	private HTableInterface baseScoreTable;// 图书基础分表
	private static final byte[] REPOCQCLASS = Bytes.toBytes("class");
	private static final byte[] REPOCQSERIESID = Bytes.toBytes("seriesid");
	private static final byte[] REPOCQORDERID = Bytes.toBytes("orderid");
	private static final byte[] REPOCQAUTHORID = Bytes.toBytes("authorid");

	private HTableInterface baseScoreFemaleTable;// 女生图书基础分表
	private HTableInterface baseScoreMaleTable;// 男生图书基础分表
	private HTableInterface baseScorePubTable;// 出版图书基础分表

	private HTableInterface editScoreTable;// 图书编辑分表
	private HTableInterface editScoreFemaleTable;// 女生图书编辑分表
	private HTableInterface editScoreMaleTable;// 男生图书编辑分表
	private HTableInterface editScorePubTable;// 出版图书编辑分表

	// private HTableInterface bookFillerTable;// 图书补白表
	// private HTableInterface bookFillerMaleTable;// 男生图书补白表
	// private HTableInterface bookFillerFemaleTable;// 女生图书补白表
	// private HTableInterface bookFillerPubTable;// 出版图书补白表

	private HTableInterface userHisTable;// 历史图书行为及黑名单表
	private HTableInterface bookBlackListTable;// 图书黑名单表

	private HTableInterface recBookTable;// 强推图书
	private static final byte[] REPOCQREC = Bytes.toBytes("rec");

	private HConnection con = null;

	private Jedis respJedis;
	private String respTable;
	private int respExpireTime;

	private String specialUserId1;
	private String specialUserId2;

	private Map<String, Float> baseScoreMap = new ConcurrentHashMap<String, Float>();
	private Map<String, String> bookClassMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
	private Map<String, Float> editScoreMap = new ConcurrentHashMap<String, Float>();
	private Map<String, BookInfo> bookInfoMap = new ConcurrentHashMap<String, BookInfo>();

	private Map<String, Float> baseScoreFemaleMap = new ConcurrentHashMap<String, Float>();
	private Map<String, String> bookClassFemaleMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
	private Map<String, Float> editScoreFemaleMap = new ConcurrentHashMap<String, Float>();
	private Map<String, BookInfo> bookInfoFemaleMap = new ConcurrentHashMap<String, BookInfo>();

	private Map<String, Float> baseScoreMaleMap = new ConcurrentHashMap<String, Float>();
	private Map<String, String> bookClassMaleMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
	private Map<String, Float> editScoreMaleMap = new ConcurrentHashMap<String, Float>();
	private Map<String, BookInfo> bookInfoMaleMap = new ConcurrentHashMap<String, BookInfo>();

	private Map<String, Float> baseScorePubMap = new ConcurrentHashMap<String, Float>();
	private Map<String, String> bookClassPubMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
	private Map<String, Float> editScorePubMap = new ConcurrentHashMap<String, Float>();
	private Map<String, BookInfo> bookInfoPubMap = new ConcurrentHashMap<String, BookInfo>();

	// 补白
	private Vector<String> fillerVector = new Vector<String>();
	private Vector<String> fillerMaleVector = new Vector<String>();
	private Vector<String> fillerFemaleVector = new Vector<String>();
	private Vector<String> fillerPubVector = new Vector<String>();
	// 图书黑名单
	private Vector<String> bookBlackVector = new Vector<String>();

	private static float baseScorePer = 0; // 各个部分的权重值，基础分，偏好分，相似分
	private static float editScorePer = 0;
	private static float prefScorePer = 0;
	private static float similarScorePer = 0;

	private static float orderFactor = 0;

	private static String editScoreFixed = null;

	private static int sameAuthorCount = 0;

	private static String editScoreFixedMale = null;
	private static String editScoreFixedFemale = null;
	private static String editScoreFixedPub = null;

	private static int hour = 0;

	private static int topN = 0;
	private HashSet<String> behaviorSet = new HashSet<String>(); // order or
																	// read or
	// 从Redis读取特别用户打印日志
	private transient Thread redisLoader = null;
	private Jedis useridsJedis;
	private String userIds = null;
	private String useridKey = null;
	private SlowLog slowLog = null; // pv
	// 定时器
	private transient Thread loader = null;

	private long readUseridInterval = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());

		baseScorePer = Float.parseFloat(appConfig.getParam("baseScorePer"));
		editScorePer = Float.parseFloat(appConfig.getParam("editScorePer"));
		prefScorePer = Float.parseFloat(appConfig.getParam("prefScorePer"));
		similarScorePer = Float.parseFloat(appConfig
				.getParam("similarScorePer"));

		orderFactor = Float.parseFloat(appConfig.getParam("orderFactor"));

		editScoreFixed = appConfig.getParam("editScore");

		editScoreFixedMale = appConfig.getParam("editScoreMale");
		editScoreFixedFemale = appConfig.getParam("editScoreFemale");
		editScoreFixedPub = appConfig.getParam("editScorePub");

		topN = Integer.parseInt((appConfig.getParam("topN")));

		sameAuthorCount = Integer.parseInt((appConfig
				.getParam("same_author_count")));

		hour = Integer.parseInt((appConfig.getParam("hour_base")));

		readUseridInterval = Long.parseLong(appConfig.getParam("interval"));

		specialUserId1 = appConfig.getParam("specialUserId1");
		specialUserId2 = appConfig.getParam("specialUserId2");

		String behaviorStr = appConfig.getParam("user_behavior"); // order,read,pv
		String[] behs = behaviorStr.split(",");
		for (String b : behs) {
			behaviorSet.add(b);
		}

		respTable = appConfig.getParam("response_table");
		respExpireTime = Integer.parseInt(appConfig
				.getParam("resp_expire_time"));

		String addrs[] = appConfig.getParam("resp_redis").split("::");
		respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		respJedis.auth(addrs[2]);
		while (respJedis == null) {
			respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
			respJedis.auth(addrs[2]);
		}

		String data[] = appConfig.getParam("userid_redis").split("::");
		useridsJedis = new Jedis(data[0], Integer.parseInt(data[1]));
		useridsJedis.auth(data[2]);
		while (useridsJedis == null) {
			useridsJedis = new Jedis(data[0], Integer.parseInt(data[1]));
			useridsJedis.auth(data[2]);
		}
		useridKey = data[3];

		// 初始化Hbase配置
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				appConfig.getParam("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort",
				appConfig.getParam("hbase.zookeeper.property.clientPort"));

		while (con == null) {
			try {
				con = HConnectionManager.createConnection(conf);
				System.out.println("MergeVersionBolt con == null L188");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 初始化Hbase表
		initHbaseTable(appConfig);

		// 定时器，每天12点从HDFS上读取图书基础分、编辑分
		if (loader == null) {
			loader = new Thread(new Runnable() {
				public void run() {
					while (true) {
						loadHbaseData();

						long sleepTime = TimeUtil
								.getMillisFromNowToTwelveOclock(hour);
						if (sleepTime > 0) {
							try {
								Thread.sleep(sleepTime);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				}
			});
			loader.setDaemon(true);
			loader.start();
		}

		// 从Redis读取要打印详细信息的用户ID
		if (redisLoader == null) {
			redisLoader = new Thread(new Runnable() {
				public void run() {
					while (true) {
						userIds = useridsJedis.rpop(useridKey);
						if (userIds != null) {
							slowLog = new SlowLog(userIds);
						}
						try {
							Thread.sleep(readUseridInterval);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			});
			redisLoader.setDaemon(true);
			redisLoader.start();
		}
	}

	private void initHbaseTable(PluginConfig appConfig) {
		// 用户图书偏好表
		while (userPrefTable == null) {
			try {
				userPrefTable = con.getTable(appConfig
						.getParam("engine_attribute_result"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 男生图书偏好表
		while (malePrefTable == null) {
			try {
				malePrefTable = con.getTable(appConfig
						.getParam("unify_male_pref"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 女生图书偏好表
		while (femalePrefTable == null) {
			try {
				femalePrefTable = con.getTable(appConfig
						.getParam("unify_female_pref"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 出版图书偏好表
		while (pubPrefTable == null) {
			try {
				pubPrefTable = con.getTable(appConfig
						.getParam("unify_publish_pref"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 用户实时图书行为表
		while (userRealTable == null) {
			try {
				userRealTable = con.getTable(appConfig
						.getParam("realpub_user_behavior"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 用户历史图书行为及黑名单表
		while (userHisTable == null) {
			try {
				userHisTable = con.getTable(appConfig
						.getParam("user_read_history"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 图书基础分表
		while (baseScoreTable == null) {
			try {
				baseScoreTable = con.getTable(appConfig
						.getParam("unify_base_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 女生图书基础分表
		while (baseScoreFemaleTable == null) {
			try {
				baseScoreFemaleTable = con.getTable(appConfig
						.getParam("unify_base_score_female"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 男生图书基础分表
		while (baseScoreMaleTable == null) {
			try {
				baseScoreMaleTable = con.getTable(appConfig
						.getParam("unify_base_score_male"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 出版图书基础分表
		while (baseScorePubTable == null) {
			try {
				baseScorePubTable = con.getTable(appConfig
						.getParam("unify_base_score_pub"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 图书编辑分表
		while (editScoreTable == null) {
			try {
				editScoreTable = con.getTable(appConfig
						.getParam("unify_edit_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 女生图书编辑分表
		while (editScoreFemaleTable == null) {
			try {
				editScoreFemaleTable = con.getTable(appConfig
						.getParam("unify_edit_score_female"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 男生图书编辑分表
		while (editScoreMaleTable == null) {
			try {
				editScoreMaleTable = con.getTable(appConfig
						.getParam("unify_edit_score_male"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 出版图书编辑分表
		while (editScorePubTable == null) {
			try {
				editScorePubTable = con.getTable(appConfig
						.getParam("unify_edit_score_pub"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 图书灰名单表
		while (userGreyBooksTable == null) {
			try {
				userGreyBooksTable = con.getTable(appConfig
						.getParam("unify_grey_list"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 用户图书黑名单表
		while (userBlackBooksTable == null) {
			try {
				userBlackBooksTable = con.getTable(appConfig
						.getParam("unify_black_list"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 强推图书表
		while (recBookTable == null) {
			try {
				recBookTable = con.getTable(appConfig
						.getParam("unify_rec_version"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		// 图书黑名单表
		while (bookBlackListTable == null) {
			try {
				bookBlackListTable = con.getTable(appConfig
						.getParam("unify_book_blacklist"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * input：user,edition,bookid,score|bookid,score|bookid,score
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		long begin = System.currentTimeMillis();
		String userId = input.getStringByField("user");
		String editionId = input.getStringByField("edition");
		String versionId = input.getStringByField("version");
		String bidScoreStr = input.getStringByField("book_scores");
		PrintHelper.print("MergeVersionBolt receive userId :" + userId
				+ " editionId:" + editionId + ">> versionId:" + versionId);

		Map<String, Float> similarScoreMap = new HashMap<String, Float>();
		if (bidScoreStr != null && !bidScoreStr.equals("")) {
			String[] bidScores = bidScoreStr.split("\\|");
			for (String bidScore : bidScores) {
				String[] tmp = bidScore.split(",");
				if (tmp.length == 2) {
					similarScoreMap.put(tmp[0], Float.parseFloat(tmp[1]));
				}
			}
		}
		if (slowLog != null) {
			slowLog.printSimilarScore(userId, similarScoreMap);
		}
		/*PrintHelper.print("MergeVersionBolt receive user: " + userId
				+ " similarScoreMap:" + similarScoreMap.size());*/

		Map<String, Float> prefScoreMap = getPrefScore(userId, versionId); // 从偏好表中查该User的图书偏好
		if (slowLog != null) {
			slowLog.printPrefScore(userId, prefScoreMap);
		}
		/*PrintHelper.print("MergeVersionBolt receive user: " + userId
				+ " prefScoreMap:" + prefScoreMap.size());*/

		Map<String, Float> bookScoreMap = new HashMap<String, Float>();

		// 混合
		bookScoreMap = mixtureCaculate(userId, similarScoreMap, prefScoreMap,
				versionId);
		if (slowLog != null) {
			slowLog.printAfterMixCal(userId, bookScoreMap);
		}

		// 过滤历史图书行为、黑名单、实时图书行为
		Set<String> filterBooksSet = filterBooks(userId, bookScoreMap);

		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after mixtureCaculate size:" + bookScoreMap.size());*/

		// filter series books
		bookScoreMap = filterSeriesBooks(bookScoreMap, versionId);
		if (slowLog != null) {
			slowLog.printAfterFilterSeriesBooks(userId, bookScoreMap);
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after filterSeriesBooks size:" + bookScoreMap.size());*/

		// 排序并取topN
		bookScoreMap = orderByScore(bookScoreMap, versionId);
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after orderByScore size:" + bookScoreMap.size());*/

		// 补白
		if (bookScoreMap.size() < topN) {
			filler(bookScoreMap, filterBooksSet, versionId);
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId + " after filler:"
				+ bookScoreMap);*/

		// 过滤同作者
		bookScoreMap = filterSameAuthor(bookScoreMap, versionId);
		if (slowLog != null) {
			slowLog.printAfterFilterSameAuthor(userId, bookScoreMap);
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after filter the same author size:" + bookScoreMap.size());*/

		StringBuffer sb = new StringBuffer();

		// 强推图书
		Random random = new Random();
		Vector<String> recVector = getRecVersionBook(userId, versionId);
		if (recVector.size() > 2) {
			int index = random.nextInt(recVector.size());
			String booka = recVector.get(index);
			recVector.remove(index);
			sb.append(booka + ",0|");
			index = random.nextInt(recVector.size());
			String bookb = recVector.get(index);
			sb.append(bookb + ",0|");
		}

		Set<String> selectBook = bookScoreMap.keySet();
		for (String book : selectBook) {
			sb.append(book + "," + bookScoreMap.get(book) + "|");
		}
		if (selectBook.size() > 1) {
			sb.deleteCharAt(sb.length() - 1);
		}

		long end = System.currentTimeMillis();
		PrintHelper.print("[" + userId + "] Excute time :" + (end - begin));
		String returnCode = respJedis.setex(respTable + ":" + userId + ":"
				+ editionId + ":" + versionId, respExpireTime, sb.toString());
		PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after save to redis return :" + returnCode);
	}

	/*
	 * 查找该用户对应版本的强推图书
	 */
	private Vector<String> getRecVersionBook(String userId, String versionId) {
		Vector<String> recVector = new Vector<String>();
		try {
			String rowKey = userId + "_" + versionId;
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = recBookTable.get(get);
			String booksStr = Bytes
					.toString(result.getValue(REPOCF, REPOCQREC));
			if (booksStr != null) {
				String[] books = booksStr.split("\\|");
				for (String book : books) {
					recVector.add(book);
				}
			}
		} catch (Exception e) {
			System.out.println("[" + userId + "] Get rec books fail...");
		}
		return recVector;
	}

	/*
	 * 过滤同系列图书
	 */
	private Map<String, Float> filterSeriesBooks(
			Map<String, Float> bookScoreMap, String version) {
		Map<String, Float> nonSerialBookMap = new HashMap<String, Float>();
		Map<Integer, BookInfo> serialBookMap = new HashMap<Integer, BookInfo>();
		Set<String> bookSet = bookScoreMap.keySet();
		Map<String, BookInfo> tmpBookInfoMap = new HashMap<String, BookInfo>();
		float maxScore = 0;
		if (version.equals("1")) { // 男生
			tmpBookInfoMap = bookInfoMaleMap;
		} else if (version.equals("2")) { // 女生
			tmpBookInfoMap = bookInfoFemaleMap;
		} else if (version.equals("3")) { // 出版
			tmpBookInfoMap = bookInfoPubMap;
		} else if (version.equals("0")) { // 首页
			tmpBookInfoMap = bookInfoMap;
		}
		for (String book : bookSet) {
			if (tmpBookInfoMap.containsKey(book)) {
				BookInfo bookInfo = tmpBookInfoMap.get(book);
				int seriesId = bookInfo.getSeriesId();
				int orderId = bookInfo.getOrderId();
				float score = bookScoreMap.get(book);
				if (score > maxScore) { // 同系列图书仅保留一本，分数为该系列图书用户的最高打分作为该书的打分
					maxScore = score;
				}
				if (seriesId != 0) {
					if (serialBookMap.containsKey(seriesId)) {
						BookInfo seriesBook = serialBookMap.get(seriesId);
						if (orderId < seriesBook.getOrderId()) {
							seriesBook = new BookInfo(book, seriesId, orderId,
									maxScore);
							serialBookMap.put(seriesId, seriesBook);
						}
					} else {
						BookInfo seriesBook = new BookInfo(book, seriesId,
								orderId, maxScore);
						serialBookMap.put(seriesId, seriesBook);
					}
				} else {
					nonSerialBookMap.put(book, score);
				}
			}
		}
		Set<Integer> serialBookSet = serialBookMap.keySet();
		Iterator<Integer> it = serialBookSet.iterator();
		while (it.hasNext()) {
			int seriesId = it.next();
			BookInfo seriesBookInfo = serialBookMap.get(seriesId);
			String bookId = seriesBookInfo.getBookId();
			float score = seriesBookInfo.getScore();
			nonSerialBookMap.put(bookId, score);
		}
		tmpBookInfoMap = null;
		serialBookMap = null;
		return nonSerialBookMap;

	}

	/*
	 * 过滤同作者图书
	 */
	private Map<String, Float> filterSameAuthor(
			Map<String, Float> bookScoreMap, String version) {
		Map<String, BookInfo> tmpBookInfoMap = new HashMap<String, BookInfo>();
		if (version.equals("1")) { // 男生
			tmpBookInfoMap = bookInfoMaleMap;
		} else if (version.equals("2")) { // 女生
			tmpBookInfoMap = bookInfoFemaleMap;
		} else if (version.equals("3")) { // 出版
			tmpBookInfoMap = bookInfoPubMap;
		} else if (version.equals("0")) { // 首页
			tmpBookInfoMap = bookInfoMap;
		}
		Map<String, Map<String, Float>> authorBooks = new HashMap<String, Map<String, Float>>();
		Map<String, Float> resultMap = new HashMap<String, Float>();
		Set<String> bookSet = bookScoreMap.keySet();
		for (String book : bookSet) {
			if (tmpBookInfoMap.containsKey(book)) {
				BookInfo bookInfo = tmpBookInfoMap.get(book);
				String authorId = bookInfo.getAuthorId();
				float score = bookScoreMap.get(book);
				if (!authorBooks.containsKey(authorId)) {
					Map<String, Float> bookScore = new HashMap<String, Float>();
					bookScore.put(book, score);
					authorBooks.put(authorId, bookScore);
				} else {
					Map<String, Float> bookScore = authorBooks.get(authorId);
					bookScore.put(book, score);
					authorBooks.put(authorId, bookScore);
				}
			}
		}
		Iterator<Map<String, Float>> bookScores = authorBooks.values()
				.iterator();
		while (bookScores.hasNext()) {
			Map<String, Float> bookScore = bookScores.next();
			bookScore = MergeUtils.sortMapByValue(bookScore, sameAuthorCount); // 同作者只取分数高的前两本
			resultMap.putAll(bookScore);
		}
		return resultMap;
	}

	/*
	 * 混合计算图书基础分、编辑分、相似分、偏好分
	 */
	private Map<String, Float> mixtureCaculate(String userId,
			Map<String, Float> similarScoreMap,
			Map<String, Float> prefScoreMap, String version) {
		Map<String, Float> bookScoreMap = new HashMap<String, Float>();

		Map<String, Float> baseScoreTmpMap = new HashMap<String, Float>();
		Map<String, Float> editScoreTmpMap = new HashMap<String, Float>();

		if (version.equals("1")) { // 男生
			baseScoreTmpMap = baseScoreMaleMap;
			editScoreTmpMap = editScoreMaleMap;
		} else if (version.equals("2")) { // 女生
			baseScoreTmpMap = baseScoreFemaleMap;
			editScoreTmpMap = editScoreFemaleMap;
		} else if (version.equals("3")) { // 出版
			baseScoreTmpMap = baseScorePubMap;
			editScoreTmpMap = editScorePubMap;
		} else if (version.equals("0")) { // 首页
			baseScoreTmpMap = baseScoreMap;
			editScoreTmpMap = editScoreMap;
		}

		// 以偏好分Map为基础计算图书的混合分
		Set<String> prefKeySet = prefScoreMap.keySet();
		Iterator<String> it = prefKeySet.iterator();
		float tempScore = 0;
		while (it.hasNext()) {
			String prefBook = it.next();
			if (similarScoreMap.containsKey(prefBook)) { // 偏好分Map中有对应的book
				tempScore = prefScoreMap.get(prefBook) * prefScorePer
						+ similarScoreMap.get(prefBook) * similarScorePer;
			} else {// 相似分Map中没有对应的book
				tempScore = prefScoreMap.get(prefBook) * prefScorePer;
			}
			if (baseScoreTmpMap.containsKey(prefBook)) { // 不在推荐库中的图书过滤掉
				tempScore += baseScoreTmpMap.get(prefBook) * baseScorePer;
				if (editScoreTmpMap.containsKey(prefBook)) {
					tempScore += editScoreTmpMap.get(prefBook) * editScorePer;
				}
				bookScoreMap.put(prefBook, tempScore);
			}
			tempScore = 0;
		}
		baseScoreTmpMap = null;
		editScoreTmpMap = null;
		return bookScoreMap;
	}

	/*
	 * 从偏好表中查该User的图书偏好,rowkey:userid,value:book1,分1|book2,分2|book3,分3|book4,分4
	 */
	public Map<String, Float> getPrefScore(String userId, String version) {
		Map<String, Float> prefScMap = new HashMap<String, Float>();
		Get get = new Get(Bytes.toBytes(userId));
		Result result = null;
		try {
			if (version.equals("1")) {
				result = malePrefTable.get(get);
			} else if (version.equals("2")) {
				result = femalePrefTable.get(get);
			} else if (version.equals("3")) {
				result = pubPrefTable.get(get);
			} else {
				result = userPrefTable.get(get);
			}
			String bookScores = Bytes.toString(result.getValue(REPOCF,
					REPOCQSCORE));
			if (bookScores != null) {
				String[] bookScore = bookScores.split("\\|");
				for (int i = 0; i < bookScore.length; i++) {
					String str = bookScore[i];
					String[] tmp = str.split(",");
					if (tmp.length == 2) {
						float prefScore = Float.parseFloat(tmp[1]);
						prefScMap.put(tmp[0], prefScore);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prefScMap;
	}

	/*
	 * 图书黑名单
	 */
	private Vector<String> getBookBlackList() {
		Vector<String> blackBooks = new Vector<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		try {
			ResultScanner rs = bookBlackListTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				blackBooks.add(bookId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return blackBooks;
	}

	/*
	 * 过滤 不属于对应版面的图书及历史图书行为、黑名单、实时图书行为
	 * 
	 * @return 返回所有需要过滤的图书
	 */
	private Set<String> filterBooks(String userId,
			Map<String, Float> bookScoreMap) {

		Set<String> realBooks = new HashSet<String>();
		Set<String> hisBlackBooks = new HashSet<String>();
		Set<String> greyBooks = new HashSet<String>();
		Set<String> blackBooks = new HashSet<String>();
		if (specialUserId1.equals(userId) || specialUserId2.equals(userId)) {
			Set<String> realBooksUser1 = getUserRealBehavior(specialUserId1); // 实时图书
			Set<String> realBooksUser2 = getUserRealBehavior(specialUserId2); // 实时图书
			realBooks.addAll(realBooksUser1);
			realBooks.addAll(realBooksUser2);

			Set<String> hisBlackBooksUser1 = getUserHisBlackBooks(specialUserId1); // 历史图书、黑名单
			Set<String> hisBlackBooksUser2 = getUserHisBlackBooks(specialUserId2); // 历史图书、黑名单
			hisBlackBooks.addAll(hisBlackBooksUser1);
			hisBlackBooks.addAll(hisBlackBooksUser2);

			Set<String> greyBooksUser1 = getUserGreyBooks(specialUserId1); // 灰名单
			Set<String> greyBooksUser2 = getUserGreyBooks(specialUserId2); // 灰名单
			greyBooks.addAll(greyBooksUser1);
			greyBooks.addAll(greyBooksUser2);

			Set<String> blackBooksUser1 = getUserBlackBooks(specialUserId1); // 黑名单
			Set<String> blackBooksUser2 = getUserBlackBooks(specialUserId2); // 黑名单
			blackBooks.addAll(blackBooksUser1);
			blackBooks.addAll(blackBooksUser2);
			
			if(slowLog != null){
				slowLog.printRealBooks(userId, realBooks);
				slowLog.printHisBlackBooks(userId, hisBlackBooks);
				slowLog.printGreyBooks(userId, greyBooks);
				slowLog.printBlackBooks(userId, blackBooks);
			}
		} else {
			realBooks = getUserRealBehavior(userId); // 实时图书
			hisBlackBooks = getUserHisBlackBooks(userId); // 历史图书、黑名单
			greyBooks = getUserGreyBooks(userId); // 灰名单
			blackBooks = getUserBlackBooks(userId); // 黑名单
			if(slowLog != null){
				slowLog.printRealBooks(userId, realBooks);
				slowLog.printHisBlackBooks(userId, hisBlackBooks);
				slowLog.printGreyBooks(userId, greyBooks);
				slowLog.printBlackBooks(userId, blackBooks);
			}
		}

		Iterator<Map.Entry<String, Float>> iterator = bookScoreMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			String book = entry.getKey();
			if (realBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (hisBlackBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (greyBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (blackBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (bookBlackVector.contains(book)) {
				iterator.remove();
			}
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ " after filter, bookScoreMap size:" + bookScoreMap.size());*/

		// 所有需要过滤的图书
		realBooks.addAll(hisBlackBooks);
		realBooks.addAll(greyBooks);
		realBooks.addAll(blackBooks);
		return realBooks;
	}

	/*
	 * 获取用户实时数据
	 */
	private Set<String> getUserRealBehavior(String userId) {
		Set<String> realBebaviorBooks = new HashSet<String>();
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(userId + "|"));
		scan.setStopRow(Bytes.toBytes(userId + "|A"));
		scan.setCaching(100);
		try {
			ResultScanner rs = userRealTable.getScanner(scan);
			for (Result result : rs) {
				String rowKey = Bytes.toString(result.getRow()); // uid|bid|pv/read/order
				String[] tmp = rowKey.split("\\|");
				if (tmp.length == 3) {
					String bid = tmp[1];
					String type = tmp[2];
					if (behaviorSet.contains(type)) {
						realBebaviorBooks.add(bid);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ ", size of user real books:" + realBebaviorBooks.size());*/
		return realBebaviorBooks;
	}

	/*
	 * 获取用户历史图书及黑名单数据
	 */
	private Set<String> getUserHisBlackBooks(String userId) {
		Set<String> hisBlackBooks = new HashSet<String>();
		if (userId.length() >= 4) {
			String rowKey = userId.substring(userId.length() - 4,
					userId.length() - 2)
					+ userId;
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = null;
			try {
				result = userHisTable.get(get);
				if (!result.isEmpty()) {// 有些用户，没有历史。
					List<Cell> cells = result.listCells();
					if (cells != null) {
						for (Cell cell : cells) {
							hisBlackBooks.add(Bytes.toString(CellUtil
									.cloneQualifier(cell)));
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		/*PrintHelper.print("MergeVersionBolt user: " + userId
				+ ", size of user his black books:" + hisBlackBooks.size());*/
		return hisBlackBooks;
	}

	/*
	 * 获取用户灰名单图书
	 */
	private Set<String> getUserGreyBooks(String userId) {
		Set<String> greyBooksSet = new HashSet<String>();
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(userId + "_"));
		scan.setStopRow(Bytes.toBytes(userId + "_A"));
		scan.setCaching(100);
		List<Delete> list = new ArrayList<Delete>();
		try {
			ResultScanner rs = userGreyBooksTable.getScanner(scan);
			for (Result result : rs) {
				String rowKey = Bytes.toString(result.getRow()); // userid_bookid
				String[] tmp = rowKey.split("_");
				String expireDate = Bytes.toString(result.getValue(REPOCF,
						REPOCQEXPIREDATE));
				int ret = TimeUtil.compareDate(expireDate);
				if (tmp.length == 2 && ret == 1) {
					greyBooksSet.add(tmp[1]);
				} else if (ret == -1) {
					Delete d1 = new Delete(rowKey.getBytes());
					list.add(d1);
				}
			}
			userGreyBooksTable.delete(list);
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*PrintHelper.print("MergeBolt user: " + userId
				+ ", size of user's grey books:" + greyBooksSet.size());*/
		return greyBooksSet;
	}

	/*
	 * 获取用户黑名单图书
	 */
	private Set<String> getUserBlackBooks(String userId) {
		Set<String> blackBooksSet = new HashSet<String>();
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(userId + "_"));
		scan.setStopRow(Bytes.toBytes(userId + "_A"));
		scan.setCaching(100);
		try {
			ResultScanner rs = userBlackBooksTable.getScanner(scan);
			for (Result result : rs) {
				String rowKey = Bytes.toString(result.getRow()); // userid_bookid
				String[] tmp = rowKey.split("_");
				if (tmp.length == 2) {
					blackBooksSet.add(tmp[1]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*PrintHelper.print("MergeBolt user: " + userId
				+ ", size of user's black books:" + blackBooksSet.size());*/
		return blackBooksSet;
	}

	/*
	 * 根据分数大小对Map进行排序
	 */
	public Map<String, Float> orderByScore(Map<String, Float> bookScoreMap,
			String version) {
		Map<String, Float> scoreMap = new HashMap<String, Float>();
		Map<String, String> bookClassTmpMap = new HashMap<String, String>();
		if (version.equals("1")) { // 男生
			bookClassTmpMap = bookClassMaleMap;
		} else if (version.equals("2")) { // 女生
			bookClassTmpMap = bookClassFemaleMap;
		} else if (version.equals("3")) { // 出版
			bookClassTmpMap = bookClassPubMap;
		} else if (version.equals("0")) { // 首页
			bookClassTmpMap = bookClassMap;
		}
		scoreMap = MergeUtils.sortByWeight(bookScoreMap, bookClassTmpMap, topN,
				orderFactor);
		bookClassTmpMap = null;
		return scoreMap;
	}

	/*
	 * 补白
	 */
	public void filler(Map<String, Float> bookScoreMap,
			Set<String> filterBooksSet, String versionId) {
		Vector<String> fillerBooks = new Vector<String>();
		if (versionId.equals("0")) {
			fillerBooks = fillerVector;
		} else if (versionId.equals("1")) {
			fillerBooks = fillerMaleVector;
		} else if (versionId.equals("2")) {
			fillerBooks = fillerFemaleVector;
		} else if (versionId.equals("3")) {
			fillerBooks = fillerPubVector;
		} else {
			return;
		}

		Iterator<String> iterator = fillerBooks.iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (filterBooksSet.contains(key)) {
				iterator.remove();
				continue;
			}
			if (bookBlackVector.contains(key)) {
				iterator.remove();
			}
		}
		int listSize = fillerBooks.size();
		int mapSize = bookScoreMap.size();
		if (listSize > 0 && listSize + mapSize > topN) {
			while (bookScoreMap.size() < topN) {
				try {
					Random random = new Random();
					String book = fillerBooks.get(random.nextInt(listSize));
					if (!bookScoreMap.containsKey(book)) {
						bookScoreMap.put(book, (float) -1);
					}
				} catch (Exception e) {
					PrintHelper
							.print("Get book from filler vector out of range...");
					return;
				}
			}
		} else if (listSize + mapSize <= topN) {
			for (int i = 0; i < fillerBooks.size(); i++) {
				String book = fillerBooks.get(i);
				if (!bookScoreMap.containsKey(book)) {
					bookScoreMap.put(book, (float) -1);
				}
			}
		}
		fillerBooks = null;
	}

	/*
	 * 加载Hbase表数据：基础分，编辑分
	 */
	protected void loadHbaseData() {
		baseScoreMap.clear();
		bookClassMap.clear();
		bookInfoMap.clear();
		baseScoreFemaleMap.clear();
		bookClassFemaleMap.clear();
		bookInfoFemaleMap.clear();
		baseScoreMaleMap.clear();
		bookClassMaleMap.clear();
		bookInfoMaleMap.clear();
		baseScorePubMap.clear();
		bookClassPubMap.clear();
		bookInfoPubMap.clear();
		try {
			Scan scan = new Scan();
			scan.setCaching(100);
			// 初始化图书基础分及图书所属分类
			ResultScanner rs = baseScoreTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String score = Bytes.toString(result.getValue(REPOCF,
						REPOCQSCORE));
				String cls = Bytes.toString(result
						.getValue(REPOCF, REPOCQCLASS));
				String seriesStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQORDERID));
				String authorId = Bytes.toString(result.getValue(REPOCF,
						REPOCQAUTHORID));
				try {
					if (score == null || "".equals(score)) {
						score = "0";
					}
					if (seriesStr == null || "".equals(seriesStr)) {
						seriesStr = "0";
					}
					if (orderStr == null || "".equals(orderStr)) {
						orderStr = "0";
					}
					float baseScore = Float.parseFloat(score);
					int seriesId = Integer.parseInt(seriesStr);
					int orderId = Integer.parseInt(orderStr);
					baseScoreMap.put(bookId, baseScore);
					bookClassMap.put(bookId, cls);
					bookInfoMap.put(bookId, new BookInfo(bookId, seriesId,
							orderId, authorId));
				} catch (Exception e) {
					PrintHelper.print("parse base score fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		PrintHelper.print("MergeVersionBolt baseScoreMap.size():"
				+ baseScoreMap.size());

		try {
			Scan scanMale = new Scan();
			scanMale.setCaching(100);
			// 初始化图书基础分及图书所属分类
			ResultScanner rs = baseScoreMaleTable.getScanner(scanMale);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String score = Bytes.toString(result.getValue(REPOCF,
						REPOCQSCORE));
				String cls = Bytes.toString(result
						.getValue(REPOCF, REPOCQCLASS));
				String seriesStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQORDERID));
				String authorId = Bytes.toString(result.getValue(REPOCF,
						REPOCQAUTHORID));
				try {
					if (score == null || "".equals(score)) {
						score = "0";
					}
					if (seriesStr == null || "".equals(seriesStr)) {
						seriesStr = "0";
					}
					if (orderStr == null || "".equals(orderStr)) {
						orderStr = "0";
					}
					float baseScore = Float.parseFloat(score);
					int seriesId = Integer.parseInt(seriesStr);
					int orderId = Integer.parseInt(orderStr);
					baseScoreMaleMap.put(bookId, baseScore);
					bookClassMaleMap.put(bookId, cls);
					bookInfoMaleMap.put(bookId, new BookInfo(bookId, seriesId,
							orderId, authorId));
				} catch (Exception e) {
					PrintHelper.print("parse male base score fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		PrintHelper.print("MergeVersionBolt baseScoreMaleMap.size():"
				+ baseScoreMaleMap.size());

		try {
			Scan scanFemale = new Scan();
			scanFemale.setCaching(100);
			// 初始化图书基础分及图书所属分类
			ResultScanner rs = baseScoreFemaleTable.getScanner(scanFemale);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String score = Bytes.toString(result.getValue(REPOCF,
						REPOCQSCORE));
				String cls = Bytes.toString(result
						.getValue(REPOCF, REPOCQCLASS));
				String seriesStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQORDERID));
				String authorId = Bytes.toString(result.getValue(REPOCF,
						REPOCQAUTHORID));
				try {
					if (score == null || "".equals(score)) {
						score = "0";
					}
					if (seriesStr == null || "".equals(seriesStr)) {
						seriesStr = "0";
					}
					if (orderStr == null || "".equals(orderStr)) {
						orderStr = "0";
					}
					float baseScore = Float.parseFloat(score);
					int seriesId = Integer.parseInt(seriesStr);
					int orderId = Integer.parseInt(orderStr);
					baseScoreFemaleMap.put(bookId, baseScore);
					bookClassFemaleMap.put(bookId, cls);
					bookInfoFemaleMap.put(bookId, new BookInfo(bookId,
							seriesId, orderId, authorId));
				} catch (Exception e) {
					PrintHelper.print("parse female base score fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		PrintHelper.print("MergeVersionBolt baseScoreFemaleMap.size():"
				+ baseScoreFemaleMap.size());

		try {
			Scan scanPub = new Scan();
			scanPub.setCaching(100);
			// 初始化图书基础分及图书所属分类
			ResultScanner rs = baseScorePubTable.getScanner(scanPub);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String score = Bytes.toString(result.getValue(REPOCF,
						REPOCQSCORE));
				String cls = Bytes.toString(result
						.getValue(REPOCF, REPOCQCLASS));
				String seriesStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(REPOCF,
						REPOCQORDERID));
				String authorId = Bytes.toString(result.getValue(REPOCF,
						REPOCQAUTHORID));
				try {
					if (score == null || "".equals(score)) {
						score = "0";
					}
					if (seriesStr == null || "".equals(seriesStr)) {
						seriesStr = "0";
					}
					if (orderStr == null || "".equals(orderStr)) {
						orderStr = "0";
					}
					float baseScore = Float.parseFloat(score);
					int seriesId = Integer.parseInt(seriesStr);
					int orderId = Integer.parseInt(orderStr);
					baseScorePubMap.put(bookId, baseScore);
					bookClassPubMap.put(bookId, cls);
					bookInfoPubMap.put(bookId, new BookInfo(bookId, seriesId,
							orderId, authorId));
				} catch (Exception e) {
					PrintHelper.print("parse publish base score fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		PrintHelper.print("MergeVersionBolt baseScorePubMap.size():"
				+ baseScorePubMap.size());

		// 初始化图书编辑分
		try {
			ResultScanner rs2 = null;
			Scan scan2 = new Scan();
			scan2.setCaching(100);
			rs2 = editScoreTable.getScanner(scan2);
			for (Result result : rs2) {
				try {
					String bookId = Bytes.toString(result.getRow());
					String scoreStr = Bytes.toString(result.getValue(REPOCF,
							REPOCQSCORE));
					float editScore = 0;
					if (scoreStr == null || scoreStr.equals("")) {
						editScore = 0;
					} else {
						if (editScoreFixed != null) { // 配置文件有该项则取配置文件中的固定值
							editScore = Float.parseFloat(editScoreFixed);
						} else {
							editScore = Float.parseFloat(scoreStr);
						}
					}
					editScoreMap.put(bookId, editScore);
				} catch (Exception e) {
					PrintHelper.print("parse edit score fail...");
					e.printStackTrace();
					continue;
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("MergeVersionBolt editScoreMap.size():"
				+ editScoreMap.size());

		try {
			ResultScanner rs2 = null;
			Scan scanEditMale = new Scan();
			scanEditMale.setCaching(100);
			rs2 = editScoreMaleTable.getScanner(scanEditMale);
			for (Result result : rs2) {
				try {
					String bookId = Bytes.toString(result.getRow());
					String scoreStr = Bytes.toString(result.getValue(REPOCF,
							REPOCQSCORE));
					float editScore = 0;
					if (scoreStr == null || scoreStr.equals("")) {
						editScore = 0;
					} else {
						if (editScoreFixedMale != null) { // 配置文件有该项则取配置文件中的固定值
							editScore = Float.parseFloat(editScoreFixedMale);
						} else {
							editScore = Float.parseFloat(scoreStr);
						}
					}
					editScoreMaleMap.put(bookId, editScore);
				} catch (Exception e) {
					PrintHelper.print("parse male edit score fail...");
					e.printStackTrace();
					continue;
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("MergeVersionBolt editScoreMaleMap.size():"
				+ editScoreMaleMap.size());

		try {
			ResultScanner rs2 = null;
			Scan scanEditFemale = new Scan();
			scanEditFemale.setCaching(100);
			rs2 = editScoreFemaleTable.getScanner(scanEditFemale);
			for (Result result : rs2) {
				try {
					String bookId = Bytes.toString(result.getRow());
					String scoreStr = Bytes.toString(result.getValue(REPOCF,
							REPOCQSCORE));
					float editScore = 0;
					if (scoreStr == null || scoreStr.equals("")) {
						editScore = 0;
					} else {
						if (editScoreFixedFemale != null) { // 配置文件有该项则取配置文件中的固定值
							editScore = Float.parseFloat(editScoreFixedFemale);
						} else {
							editScore = Float.parseFloat(scoreStr);
						}
					}
					editScoreFemaleMap.put(bookId, editScore);
				} catch (Exception e) {
					PrintHelper.print("parse female edit score fail...");
					e.printStackTrace();
					continue;
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("MergeVersionBolt editScoreFemaleMap.size():"
				+ editScoreFemaleMap.size());

		try {
			ResultScanner rs2 = null;
			Scan scanEditPub = new Scan();
			scanEditPub.setCaching(100);
			rs2 = editScorePubTable.getScanner(scanEditPub);
			for (Result result : rs2) {
				try {
					String bookId = Bytes.toString(result.getRow());
					String scoreStr = Bytes.toString(result.getValue(REPOCF,
							REPOCQSCORE));
					float editScore = 0;
					if (scoreStr == null || scoreStr.equals("")) {
						editScore = 0;
					} else {
						if (editScoreFixedPub != null) { // 配置文件有该项则取配置文件中的固定值
							editScore = Float.parseFloat(editScoreFixedPub);
						} else {
							editScore = Float.parseFloat(scoreStr);
						}
					}
					editScorePubMap.put(bookId, editScore);
				} catch (Exception e) {
					PrintHelper.print("parse publish edit score fail...");
					e.printStackTrace();
					continue;
				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("MergeVersionBolt editScorePubMap.size():"
				+ editScorePubMap.size());

		// 初始化补白列表
		fillerVector = new Vector<String>(baseScoreMap.keySet());
		/*PrintHelper.print("MergeVersionBolt fillerVector.size():"
				+ fillerVector.size());*/

		fillerMaleVector = new Vector<String>(baseScoreMaleMap.keySet());
		/*PrintHelper.print("MergeVersionBolt fillerMaleVector.size():"
				+ fillerMaleVector.size());*/

		fillerFemaleVector = new Vector<String>(baseScoreFemaleMap.keySet());
		/*PrintHelper.print("MergeVersionBolt fillerFemaleVector.size():"
				+ fillerFemaleVector.size());*/

		fillerPubVector = new Vector<String>(baseScorePubMap.keySet());
		/*PrintHelper.print("MergeVersionBolt fillerPubVector.size():"
				+ fillerPubVector.size());*/

		bookBlackVector = getBookBlackList();
		/*PrintHelper.print("MergeVersionBolt bookBlackVector.size():"
				+ bookBlackVector.size());*/

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
