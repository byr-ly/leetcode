package com.eb.bi.rs.mras.unifyrec.realtimerec.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mras.unifyrec.realtimerec.domain.BookInfo;
import com.eb.bi.rs.mras.unifyrec.realtimerec.util.MergeUtils;
import com.eb.bi.rs.mras.unifyrec.realtimerec.util.SlowLog;
import com.eb.bi.rs.mras.unifyrec.realtimerec.util.TimeUtil;

/**
 * 用于处理推荐结果：混合，过滤，排序，补白
 * 
 * @author ynn
 * @date 创建时间：2015-10-20 上午11:00:50
 * @version 1.0
 */
public class MergeBoltSimilar extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private HTableInterface userPrefTable; // 用户偏好图书表
	private HTableInterface userRealTable;// 实时图书行为表
	private HTableInterface userGreyBooksTable;// 用户灰名单表
	private HTableInterface userBlackBooksTable;// 用户黑名单表
	private HTableInterface baseScoreTable;// 图书基础分表
	private HTableInterface editScoreTable;// 图书编辑分表
	private HTableInterface bookFillerTable;// 图书补白表
	private HTableInterface userHisTable;// 历史图书行为及黑名单表
	private HTableInterface bookBlackListTable;// 图书黑名单表
	private HTableInterface recRepTable; // 推荐库 图书ID|定制标签|版面集
	private HTableInterface userDelBooksTable; // 用户删除的图书
	private HTableInterface userClassWeightTable; // 用户分类权重表
	private HTableInterface userReqTable; // 用户请求表

	private Jedis respJedis;
	private String respTable;
	private int respExpireTime;

	private String specialUserId1;
	private String specialUserId2;

	// 图书基础分，编辑分
	private Map<String, Float> baseScoreMap = new ConcurrentHashMap<String, Float>();
	private Map<String, String> bookClassMap = new ConcurrentHashMap<String, String>(); // 图书属于哪个类型
	private Map<String, Float> editScoreMap = new ConcurrentHashMap<String, Float>();
	private Map<String, BookInfo> bookInfoMap = new ConcurrentHashMap<String, BookInfo>();

	private Map<String, ArrayList<String>> classFillerMap = new ConcurrentHashMap<String, ArrayList<String>>();

	// 补白
	private Vector<String> fillerVector = new Vector<String>();
	// 图书黑名单
	private Set<String> bookBlackSet = new HashSet<String>();

	private static float baseScorePer = 0; // 各个部分的权重值，基础分，偏好分，相似分
	private static float editScorePer = 0;
	private static float prefScorePer = 0;
	private static float similarScorePer = 0;

	private static float orderFactor = 0;

	private static String editScoreFixed = null;

	private static int sameAuthorCount = 0;

	private static int hour = 0;

	private static int topN = 0;
	private static int classNum = 0;
	private static float k1 = 0;
	private static float sumWeight = 0;
	//private Map<String, Map<String, Float>> classBooksMap = new ConcurrentHashMap<String, Map<String, Float>>();

	private HashSet<String> behaviorSet = new HashSet<String>();

	private Map<Integer, ArrayList<String>> editionRepoListMap = new ConcurrentHashMap<Integer, ArrayList<String>>();

	// 定时器
	private transient Thread loader = null;

	// 从Redis读取特别用户打印日志
	private transient Thread redisLoader = null;
	private Jedis useridsJedis;
	private String userIds = null;
	private String useridKey = null;
	private SlowLog slowLog = null;

	String[] addrs = null;

	private boolean isFirstLoad = true;

	private long readUseridInterval = 0;
	/*-----------------20170119临时代码--------------*/
	private String lingdaoBookScores = null;
	
	private String filterLevel = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());
		/*-----------------20170119临时代码--------------*/
		lingdaoBookScores = appConfig.getParam("lingdaoBookScores");
		baseScorePer = Float.parseFloat(appConfig.getParam("baseScorePer"));
		editScorePer = Float.parseFloat(appConfig.getParam("editScorePer"));
		prefScorePer = Float.parseFloat(appConfig.getParam("prefScorePer"));
		similarScorePer = Float.parseFloat(appConfig
				.getParam("similarScorePer"));

		orderFactor = Float.parseFloat(appConfig.getParam("orderFactor"));

		editScoreFixed = appConfig.getParam("editScore");

		topN = Integer.parseInt((appConfig.getParam("topN")));
		classNum = Integer.parseInt((appConfig.getParam("classNum")));
		k1 = Float.parseFloat((appConfig.getParam("k1")));
		sumWeight = Float.parseFloat((appConfig.getParam("sumWeight")));

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
		
		filterLevel = appConfig.getParam("filter_level");

		respTable = appConfig.getParam("response_table");
		respExpireTime = Integer.parseInt(appConfig
				.getParam("resp_expire_time"));

		addrs = appConfig.getParam("resp_redis").split("::");
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
		HConnection con = null;
		while (con == null) {
			try {
				con = HConnectionManager.createConnection(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 用户图书偏好表
		while (userPrefTable == null) {
			try {
				userPrefTable = con.getTable(appConfig
						.getParam("engine_attribute_result"));
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

		// 用户删除的图书表
		while (userDelBooksTable == null) {
			try {
				userDelBooksTable = con.getTable(appConfig
						.getParam("realpub_user_delete_info"));
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

		// 图书编辑分表
		while (editScoreTable == null) {
			try {
				editScoreTable = con.getTable(appConfig
						.getParam("unify_edit_score"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 图书补白库表
		while (bookFillerTable == null) {
			try {
				bookFillerTable = con.getTable(appConfig
						.getParam("unify_book_filler"));
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

		while (recRepTable == null) {
			try {
				recRepTable = con.getTable(appConfig
						.getParam("unified_rec_rep"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 用户分类权重表
		while (userClassWeightTable == null) {
			try {
				userClassWeightTable = con.getTable(appConfig
						.getParam("unify_user_classid_weight"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 用户请求表
		while (userReqTable == null) {
			try {
				userReqTable = con.getTable(appConfig
						.getParam("unify_user_req"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// 查Hbase表，获得每个edition对应的books
		// getBooksWithEdition();

		// 定时器，每天4点从Hbase中读取图书基础分、编辑分、
		if (loader == null) {
			loader = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						baseScoreMap.clear();
						bookClassMap.clear();
						bookInfoMap.clear();

						long maxStamp = System.currentTimeMillis();
						long minStamp = maxStamp - 24 * 60 * 60 * 1000;
						int sum = 0, count = 0;
						while (sum < 10000 && count < 10) {
							sum += queryBaseScore(minStamp, maxStamp);
							count++;
							minStamp -= 24 * 60 * 60 * 1000;
							maxStamp -= 24 * 60 * 60 * 1000;
						}
						// 取了前10天的数据还小于1万，则取全表
						if (sum < 10000) {
							queryBaseScore(0, maxStamp);
						}
						PrintHelper.print("MergeBolt baseScoreMap.size():"
								+ baseScoreMap.size());

						// 初始化图书编辑分
						try {
							ResultScanner rs2 = null;
							Scan scan2 = new Scan();
							scan2.setCaching(100);
							rs2 = editScoreTable.getScanner(scan2);
							for (Result result : rs2) {
								try {
									String bookId = Bytes.toString(result
											.getRow());
									String scoreStr = Bytes.toString(result
											.getValue(Constant.REPOCF,
													Constant.REPOCQSCORE));
									float editScore = 0;
									if (scoreStr == null || scoreStr.equals("")) {
										editScore = 0;
									} else {
										if (editScoreFixed != null) { // 配置文件有该项则取配置文件中的固定值
											editScore = Float
													.parseFloat(editScoreFixed);
										} else {
											editScore = Float
													.parseFloat(scoreStr);
										}
									}
									editScoreMap.put(bookId, editScore);
								} catch (Exception e) {
									PrintHelper
											.print("parse edit score fail...");
									e.printStackTrace();
									continue;
								}

							}
						} catch (IOException e1) {
							e1.printStackTrace();
						}
						PrintHelper.print("MergeBolt editScoreMap.size():"
								+ editScoreMap.size());

						// 初始化补白列表
						fillerVector = getFillerBook();
						PrintHelper.print("MergeBolt fillerVector.size():"
								+ fillerVector.size());

						// 初始化图书黑名单表
						bookBlackSet = getBookBlackList();
						PrintHelper.print("MergeBolt bookBlackSet.size():"
								+ bookBlackSet.size());

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
				@Override
				public void run() {
					while (true) {
						// userIds = useridsJedis.rpop(useridKey);
						userIds = useridsJedis.lindex(useridKey, 0);
						if (userIds != null) {
							slowLog = new SlowLog(userIds);
						} else {
							slowLog = null;
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

	// 获取基础分表数据
	protected int queryBaseScore(long minStamp, long maxStamp) {
		int count = 0;
		try {
			Scan scan1 = new Scan();
			scan1.setCaching(100);
			// 设置查询数据的时间范围
			scan1.setTimeRange(minStamp, maxStamp);
			// 初始化图书基础分及图书所属分类
			ResultScanner rs = baseScoreTable.getScanner(scan1);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String score = Bytes.toString(result.getValue(Constant.REPOCF,
						Constant.REPOCQSCORE));
				String cls = Bytes.toString(result.getValue(Constant.REPOCF,
						Constant.REPOCQCLASS));
				String seriesStr = Bytes.toString(result.getValue(
						Constant.REPOCF, Constant.REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(
						Constant.REPOCF, Constant.REPOCQORDERID));
				String authorId = Bytes.toString(result.getValue(
						Constant.REPOCF, Constant.REPOCQAUTHORID));
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

					// 把推荐补白库按分类分组
					if (classFillerMap.containsKey(cls)) {
						classFillerMap.get(cls).add(bookId);
					} else {
						ArrayList<String> classList = new ArrayList<String>();
						classList.add(bookId);
						classFillerMap.put(cls, classList);
					}
					bookInfoMap.put(bookId, new BookInfo(bookId, seriesId,
							orderId, authorId));
					count++;
				} catch (Exception e) {
					PrintHelper.print("parse base score fail...");
					e.printStackTrace();
					continue;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return count;
	}

	/*
	 * input：user,edition,bookid,score|bookid,score|bookid,score
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long beginTime = System.currentTimeMillis();

		String userId = input.getStringByField("user");
		String editionId = input.getStringByField("edition");
		String bidScoreStr = input.getStringByField("book_scores");
		PrintHelper.print("MergeBolt receive userId :" + userId + " editionId:"
				+ editionId);

		// 保存用户请求所带的edition_id
		saveUserReq(userId, editionId);

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

		Map<String, Float> prefScoreMap = getPrefScore(userId); // 从偏好表中查该User的图书偏好

		if (slowLog != null) {
			slowLog.printPrefScore(userId, prefScoreMap);
		}

		Map<String, Float> bookScoreMap = new HashMap<String, Float>();
		Map<String, Float> resultMap = new LinkedHashMap<String, Float>();

		List<String> bookWithEditionList = editionRepoListMap.get(editionId);

		// 过滤不属于对应editionId版面的图书及历史图书行为、黑名单、实时图书行为
		Set<String> filterBooksSet = filterBooks(userId, bookWithEditionList,
				similarScoreMap, prefScoreMap);

		// 混合
		bookScoreMap = mixtureCaculate(userId, similarScoreMap, prefScoreMap);
		if (slowLog != null) {
			slowLog.printAfterMixCal(userId, bookScoreMap);
		}

		/*-----------------20170119临时代码--------------*/
		if (userId.equals("13901368686")) {
			bookScoreMap.clear();
			// String bookScore = "book1,score|book2,score";
			String[] bookscores = lingdaoBookScores.split("\\|", -1);
			// 随机获取其中的200个
			if (bookscores.length > 200) {
				Random random = new Random();
				int count = 0;
				while (bookScoreMap.size() < 200 && count < 1000) {
					int x = random.nextInt(bookscores.length);
					String[] bookvalue = bookscores[x].split(",", -1);
					if (bookvalue.length >= 2) {
						bookScoreMap.put(bookvalue[0],
								Float.valueOf(bookvalue[1]));
					}
					count++;
				}

			} else {
				// 直接处理
				for (int i = 0; i < bookscores.length; i++) {
					String[] bookvalue = bookscores[i].split(",", -1);
					if (bookvalue.length >= 2) {
						bookScoreMap.put(bookvalue[0],
								Float.valueOf(bookvalue[1]));
					}
				}
			}
		}
		/*-----------------20170119临时代码--------------*/

		// filter series books
		bookScoreMap = filterSeriesBooks(bookScoreMap);
		if (slowLog != null) {
			slowLog.printAfterFilterSeriesBooks(userId, bookScoreMap);
		}

		// 过滤同作者
		bookScoreMap = filterSameAuthor(bookScoreMap);
		if (slowLog != null) {
			slowLog.printAfterFilterSameAuthor(userId, bookScoreMap);
		}

		// 把bookScoreMap按分类分组
		Set<Entry<String, Float>> bookScoreSet = bookScoreMap.entrySet();
		Iterator<Entry<String, Float>> it = bookScoreSet.iterator();
		String bookStr = "";
		
		String category = "";
		
		Map<String, Map<String, Float>> classBooksMap = new HashMap<String, Map<String, Float>>();
		while (it.hasNext()) {
			Entry<String, Float> bookScoreEntry = it.next();
			bookStr = bookScoreEntry.getKey();
			category = bookClassMap.get(bookStr);
			if (classBooksMap.containsKey(category)) {
				classBooksMap.get(category).put(bookStr,
						bookScoreEntry.getValue());
			} else {
				Map<String, Float> classMap = new HashMap<String, Float>();
				classMap.put(bookStr, bookScoreEntry.getValue());
				classBooksMap.put(category, classMap);
			}
		}

		// 查找该用户对应的分类权重
		Map<String, Float> userClassWeight = getUserClassWeight(userId);
		
		// 100+应推图书池
		Map<String, Float> recPool = new HashMap<String, Float>();
		Map<String, Map<String, Float>> classRecBooks = new HashMap<String, Map<String, Float>>();
		Map<String, Float> tmpMap = new HashMap<String, Float>(); //用于存放每个分类对应的图书
		Set<String> classSet = userClassWeight.keySet(); 
		int recNum = 0;
		int mapSize = 0;
		for (String cla : classSet) {
			tmpMap = new HashMap<String, Float>();
			recNum = (int) (100 * userClassWeight.get(cla));
			Map<String, Float> map = classBooksMap.get(cla);
			if (map != null && map.size() >= recNum){
				map = MergeUtils.sortMapByValue(map, recNum);
				recPool.putAll(map);
				tmpMap.putAll(map);
			} else {
				mapSize = 0;
				if(map != null){
					recPool.putAll(map);
					tmpMap.putAll(map);
					mapSize = map.size();
				}
				recNum = recNum - mapSize;
				if(classFillerMap.get(cla) == null){
					classRecBooks.put(cla, tmpMap);
					continue;
				}
				List<String> fillerList = new CopyOnWriteArrayList<String>(classFillerMap.get(cla));
				Random random = new Random();
				int size = fillerList.size();
				if (size <= recNum) {
					for (String book : fillerList) {
						recPool.put(book, (float) -1);
						tmpMap.put(book, (float) -1);
					}
				} else {
					for (int i = 0, n = 0; i < recNum && n < 2*recNum; n++) {
						int index = random.nextInt(size);
						String book = fillerList.get(index);
						if (!recPool.containsKey(book)) {
							recPool.put(book, (float) -1);
							tmpMap.put(book, (float) -1);
							i++;
						}
					}
				}
			}
			classRecBooks.put(cla, tmpMap);
		}
		if (slowLog != null) {
			slowLog.printRecPool(userId, recPool);
		}		
		// 将分类权重降序排列,对累计权重达到80%部分的分类进行推荐,且分类个数≤10
		userClassWeight = MergeUtils.sortMapByValue(userClassWeight, sumWeight,
				classNum);
		if (slowLog != null) {
			slowLog.printUserClassWeight(userId, userClassWeight);
		}
		
		int eachRecNum = 0;
		Set<String> orderedClassSet = userClassWeight.keySet();
		for (String orderedClass : orderedClassSet) {
			eachRecNum = (int) Math.ceil(k1 * userClassWeight.get(orderedClass));
			Map<String, Float> map1 = classRecBooks.get(orderedClass);
			if (map1 != null && map1.size() >= eachRecNum) {
				map1 = MergeUtils.sortMapByValue(map1, eachRecNum);
				resultMap.putAll(map1);
			} 
		}
		System.out.println("=====resultMap: " + resultMap);

		int resultMapSize = resultMap.size();

		// 过滤第一版块中推荐的K1本图书bookScoreMap
		Set<String> bookSet = new HashSet<String>(recPool.keySet());
		Iterator<Map.Entry<String, Float>> iterator = resultMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			String book = entry.getKey();
			if (bookSet.contains(book)) {
				recPool.remove(book);
			}
		}

		// 排序并取topN
		recPool = orderByScore(recPool, topN - resultMapSize);
		if (slowLog != null) {
			slowLog.printAfterTopN(userId, recPool);
		}

		// 补白
		filterBooksSet.addAll(resultMap.keySet());
		if (recPool.size() < topN - resultMapSize) {
			filler(recPool, bookWithEditionList, filterBooksSet);
		}
		if (slowLog != null) {
			slowLog.printFiller(userId, recPool);
		}

		StringBuffer sb = new StringBuffer();
		resultMap.putAll(recPool);
		Set<String> geneBook = resultMap.keySet();
		for (String book : geneBook) {
			sb.append(book + "," + resultMap.get(book) + "|");
		}

		if (geneBook.size() > 1) {
			sb.deleteCharAt(sb.length() - 1);
		}
		if (slowLog != null) {
			slowLog.printFinalResult(userId, sb);
		}
		String returnCode = "";
		try {
			returnCode = respJedis.setex(respTable + ":" + userId + ":"
					+ editionId, respExpireTime, sb.toString());
		} catch (JedisConnectionException e) {
			PrintHelper.print("JedisConnectionException[setex] is catched");
			e.printStackTrace();
			if (!respJedis.isConnected()) {
				try {
					respJedis.connect();
					returnCode = respJedis.setex(respTable + ":" + userId + ":"
							+ editionId, respExpireTime, sb.toString());
				} catch (JedisConnectionException e1) {
					PrintHelper
							.print("JedisConnectionException[connect] is catched");
					e1.printStackTrace();
				}
			}
		}
		PrintHelper.print("=====User : " + userId+ ", MergeBolt Total Costs : "
				+ (System.currentTimeMillis() - beginTime) 
				+ ", After save to redis return :" + returnCode);
	}

	/*
	 * 保存用户请求，key:userid， value:last editionid_this editionid
	 */
	private void saveUserReq(String userid, String editionId) {
		try {
			Get get = new Get(Bytes.toBytes(userid));
			Result result = userReqTable.get(get);
			String value = Bytes.toString(result.getValue(Constant.REPOCF,
					Constant.REPOREQ_EDITION));
			if (value == null || "".equals(value)) {
				value = "7_7";
			}
			String[] vals = value.split("_");
			if (vals.length == 2) {
				value = vals[1] + "_" + editionId;
			} else {
				value = "7_" + editionId;
			}

			Put put = new Put(Bytes.toBytes(userid));
			put.add(Constant.REPOCF, Constant.REPOREQ_EDITION,
					Bytes.toBytes(value));

			userReqTable.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 过滤同系列图书
	 */
	private Map<String, Float> filterSeriesBooks(Map<String, Float> bookScoreMap) {
		Map<String, Float> nonSerialBookMap = new HashMap<String, Float>();
		Map<Integer, BookInfo> serialBookMap = new HashMap<Integer, BookInfo>();
		Set<String> bookSet = bookScoreMap.keySet();
		Map<Integer, Float> serialMaxScoreMap = new HashMap<Integer, Float>();
		float maxScore = 0;
		for (String book : bookSet) {
			if (bookInfoMap.containsKey(book)) {
				BookInfo bookInfo = bookInfoMap.get(book);
				int seriesId = bookInfo.getSeriesId();
				int orderId = bookInfo.getOrderId();
				float score = bookScoreMap.get(book);
				if (seriesId != 0) {
					if (serialBookMap.containsKey(seriesId)) {
						maxScore = serialMaxScoreMap.get(seriesId);
						if (score > maxScore) { // 同系列图书仅保留一本，分数为该系列图书用户的最高打分作为该书的打分
							maxScore = score;
						}
						BookInfo seriesBook = serialBookMap.get(seriesId);
						if (orderId < seriesBook.getOrderId()) {
							seriesBook = new BookInfo(book, seriesId, orderId,
									maxScore);
							serialBookMap.put(seriesId, seriesBook);
							serialMaxScoreMap.put(seriesId, maxScore);
						}
					} else {
						BookInfo seriesBook = new BookInfo(book, seriesId,
								orderId, score);
						serialBookMap.put(seriesId, seriesBook);
						serialMaxScoreMap.put(seriesId, score);
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
		return nonSerialBookMap;

	}

	/*
	 * 过滤同作者图书
	 */
	private Map<String, Float> filterSameAuthor(Map<String, Float> bookScoreMap) {
		Map<String, Map<String, Float>> authorBooks = new HashMap<String, Map<String, Float>>();
		Map<String, Float> resultMap = new HashMap<String, Float>();
		Set<String> bookSet = bookScoreMap.keySet();
		for (String book : bookSet) {
			if (bookInfoMap.containsKey(book)) {
				BookInfo bookInfo = bookInfoMap.get(book);
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
			Map<String, Float> similarScoreMap, Map<String, Float> prefScoreMap) {
		Map<String, Float> bookScoreMap = new HashMap<String, Float>();
		// 计算相似Map中所有图书的混合分
		Set<String> similarKeySet = similarScoreMap.keySet();
		Iterator<String> it = similarKeySet.iterator();
		float tempScore = 0;
		while (it.hasNext()) {
			String book = it.next();
			if (prefScoreMap.containsKey(book)) { // 偏好分Map中有对应的book
				tempScore = prefScoreMap.get(book) * prefScorePer
						+ similarScoreMap.get(book) * similarScorePer;
				prefScoreMap.remove(book);// 从偏好Map中删除在相似Map中有的图书
				if (baseScoreMap.containsKey(book)) { // 不在推荐库中的图书过滤掉
					tempScore += baseScoreMap.get(book) * baseScorePer;
					if (editScoreMap.containsKey(book)) {
						tempScore += editScoreMap.get(book) * editScorePer;
					}
					bookScoreMap.put(book, tempScore);
				}
				tempScore = 0;
			} else {// 偏好分Map中没有对应的book
				tempScore = similarScoreMap.get(book) * similarScorePer;
				if (baseScoreMap.containsKey(book)) { // 不在推荐库中的图书过滤掉
					tempScore += baseScoreMap.get(book) * baseScorePer;
					if (editScoreMap.containsKey(book)) {
						tempScore += editScoreMap.get(book) * editScorePer;
					}
					bookScoreMap.put(book, tempScore);
				}
				tempScore = 0;
			}
		}

		// 计算在偏好Map中但不在相似Map中的图书混合分数
		Set<String> prefKeySet = prefScoreMap.keySet();
		it = prefKeySet.iterator();
		tempScore = 0;
		while (it.hasNext()) {
			String prefBook = it.next();
			tempScore = prefScoreMap.get(prefBook) * prefScorePer;
			if (baseScoreMap.containsKey(prefBook)) { // 不在推荐库中的图书过滤掉
				tempScore += baseScoreMap.get(prefBook) * baseScorePer;
				if (editScoreMap.containsKey(prefBook)) {
					tempScore += editScoreMap.get(prefBook) * editScorePer;
				}
				bookScoreMap.put(prefBook, tempScore);
			}
			tempScore = 0;
		}
		return bookScoreMap;
	}

	/*
	 * 从偏好表中查该User的图书偏好,rowkey:userid,value:book1,分1|book2,分2|book3,分3|book4,分4
	 */
	public Map<String, Float> getPrefScore(String userId) {
		Map<String, Float> prefScMap = new HashMap<String, Float>();
		Get get = new Get(Bytes.toBytes(userId));
		Result result = null;
		try {
			result = userPrefTable.get(get);
			String bookScores = Bytes.toString(result.getValue(Constant.REPOCF,
					Constant.REPOCQSCORE));
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
	 * 补白图书数据
	 */
	private Vector<String> getFillerBook() {
		Vector<String> fillerBooks = new Vector<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		try {
			ResultScanner rs = bookFillerTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				fillerBooks.add(bookId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (isFirstLoad) { // 是否是第一次加载
			isFirstLoad = false;
			return fillerBooks;
		}

		if (fillerBooks.size() > 0) {
			return fillerBooks;
		}

		return fillerVector;
	}

	/*
	 * 图书黑名单
	 */
	private Set<String> getBookBlackList() {
		Set<String> blackBooks = new HashSet<String>();
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
	 * 过滤 不属于对应版面的图书及历史图书行为、黑名单、实时图书行为、灰名单
	 * 
	 * @return 返回所有需要过滤的图书
	 */
	private Set<String> filterBooks(String userId,
			List<String> bookWithEditionList,
			Map<String, Float> similarScoreMap, Map<String, Float> prefScoreMap) {

		Set<String> realBooks = new HashSet<String>();
		Set<String> hisBlackBooks = new HashSet<String>();
		Set<String> greyBooks = new HashSet<String>();
		Set<String> blackBooks = new HashSet<String>();
		Set<String> delBooks = new HashSet<String>();
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

			Set<String> delBooksUser1 = getUserDelBooks(specialUserId1); // 删除的图书
			Set<String> delBooksUser2 = getUserDelBooks(specialUserId2); // 删除的图书
			delBooks.addAll(delBooksUser1);
			delBooks.addAll(delBooksUser2);

			if (slowLog != null) {
				slowLog.printRealBooks(userId, realBooks);
				slowLog.printHisBlackBooks(userId, hisBlackBooks);
				slowLog.printGreyBooks(userId, greyBooks);
				slowLog.printBlackBooks(userId, blackBooks);
				slowLog.printDelBooks(userId, delBooks);
			}

		} else {
			realBooks = getUserRealBehavior(userId); // 实时图书
			hisBlackBooks = getUserHisBlackBooks(userId); // 历史图书、黑名单
			greyBooks = getUserGreyBooks(userId); // 灰名单
			blackBooks = getUserBlackBooks(userId); // 黑名单
			delBooks = getUserDelBooks(userId); // 删除的图书

			if (slowLog != null) {
				slowLog.printRealBooks(userId, realBooks);
				slowLog.printHisBlackBooks(userId, hisBlackBooks);
				slowLog.printGreyBooks(userId, greyBooks);
				slowLog.printBlackBooks(userId, blackBooks);
				slowLog.printDelBooks(userId, delBooks);
			}
		}

		Iterator<Map.Entry<String, Float>> iterator = similarScoreMap
				.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			String book = entry.getKey();
			/*
			 * if (!bookWithEditionList.contains(book)) { iterator.remove();
			 * continue; }
			 */
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
			if (delBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (bookBlackSet.contains(book)) {
				iterator.remove();
			}
		}

		iterator = prefScoreMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Float> entry = iterator.next();
			String book = entry.getKey();
			/*
			 * if (!bookWithEditionList.contains(book)) { iterator.remove();
			 * continue; }
			 */
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
			if (delBooks.contains(book)) {
				iterator.remove();
				continue;
			}
			if (bookBlackSet.contains(book)) {
				iterator.remove();
			}
		}
		/*
		 * PrintHelper.print("MergeBolt user: " + userId +
		 * " after prefScore filter size:" + prefScoreMap.size());
		 */

		// 所有需要过滤的图书
		realBooks.addAll(hisBlackBooks);
		realBooks.addAll(greyBooks);
		realBooks.addAll(blackBooks);
		realBooks.addAll(delBooks);
		realBooks.addAll(bookBlackSet);
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
		return realBebaviorBooks;
	}

	/*
	 * 获取用户分类权重
	 */
	private Map<String, Float> getUserClassWeight(String userId) {
		Map<String, Float> userClassWeight = new HashMap<String, Float>();
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(userId + "_"));
		scan.setStopRow(Bytes.toBytes(userId + "_A"));
		scan.setCaching(100);
		try {
			ResultScanner rs = userClassWeightTable.getScanner(scan);
			String rowKey = "";
			String[] tmp = null;
			String weightStr = "";
			float weight = 0;
			for (Result result : rs) {
				rowKey = Bytes.toString(result.getRow()); // uid_classid
				tmp = rowKey.split("_");
				weightStr = Bytes.toString(result.getValue(
						Constant.REPOCF, Constant.REPOWEIGHT));
				weight = Float.parseFloat(weightStr);
				if (tmp.length == 2 && weight != 0) {
					String classId = tmp[1];
					userClassWeight.put(classId, weight);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userClassWeight;
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
			String level = null;
			try {
				result = userHisTable.get(get);
				if (!result.isEmpty()) {// 有些用户，没有历史。
					List<Cell> cells = result.listCells();
					if (cells != null) {
						for (Cell cell : cells) {
							level = Bytes.toString(CellUtil.cloneValue(cell));
							if(filterLevel.equals(level)){ // 过滤级别
								hisBlackBooks.add(Bytes.toString(CellUtil
										.cloneQualifier(cell)));
							}
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
				String expireDate = Bytes.toString(result.getValue(
						Constant.REPOCF, Constant.REPOCQEXPIREDATE));
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

		/*
		 * PrintHelper.print("MergeBolt user: " + userId +
		 * ", size of user's grey books:" + greyBooksSet.size());
		 */
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

		return blackBooksSet;
	}

	/*
	 * 获取用户删除图书
	 */
	private Set<String> getUserDelBooks(String userId) {
		Set<String> delBooksSet = new HashSet<String>();
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(userId + "_"));
		scan.setStopRow(Bytes.toBytes(userId + "_A"));
		scan.setCaching(100);
		try {
			ResultScanner rs = userDelBooksTable.getScanner(scan);
			for (Result result : rs) {
				String rowKey = Bytes.toString(result.getRow()); // userid_bookid
				String[] tmp = rowKey.split("_");
				if (tmp.length == 2) {
					delBooksSet.add(tmp[1]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return delBooksSet;
	}

	/*
	 * 根据分数大小对Map进行排序
	 */
	public Map<String, Float> orderByScore(Map<String, Float> bookScoreMap,
			int size) {
		Map<String, Float> scoreMap = new HashMap<String, Float>();
		scoreMap = MergeUtils.sortByWeight(bookScoreMap, bookClassMap, size,
				orderFactor);
		return scoreMap;
	}

	/*
	 * 补白
	 */
	public void filler(Map<String, Float> bookScoreMap,
			List<String> bookWithEditionList, Set<String> filterBooksSet) {
		Vector<String> fillerBooks = new Vector<String>(fillerVector);
		Iterator<String> iterator = fillerBooks.iterator();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (filterBooksSet.contains(key)) {
				iterator.remove();
				continue;
			}
			if (bookBlackSet.contains(key)) {
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
					if (!bookScoreMap.containsKey(book)
					/* && bookWithEditionList.contains(book) */) {
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
				if (!bookScoreMap.containsKey(book)
				/* && bookWithEditionList.contains(book) */) {
					bookScoreMap.put(book, (float) -1);
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
