package com.eb.bi.rs.mrasstorm.correrecrealtimefilter;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import scala.collection.mutable.StringBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;

public class NewReadFilterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	private HTable hTable;

	private Jedis inputJedis;
	private Jedis outputJedis;

	private int recNumber;
	private int orderRecNumber;
	private int readRecNumber;
	private int resultExpireTime;
	private String resultTable;
	private String orderType;
	private String readType;
	private String browseType;
	
	private HTable userStatisticsInfoTable;// 用户统计信息表
	
	private HTable bookFeatureInfoTable;// 图书特征信息表
	private ConcurrentHashMap<String,String> bookidAndLowvalMap = new ConcurrentHashMap<String,String>();//图书特征信息map
	//private HashMap<String,Double> bookidAndLowvalueMap = new HashMap<String,Double>();//图书id和雅俗值map
	
	private HTable bookCorrelationSimTable;// 图书关联相似度信息表
	//private Map<String,String> bookCorrelationSimMap = new HashMap<String,String>();//图书关联相似度map
	
	private HTable classBigclassTable;// 分类大类对应表
	
	private HTable bookBlackListTable;// 图书黑名单表
	private Vector<String> blackBooks = new Vector<String>();
	
	private HTable browseClassBookTable;// 浏览分类热书表
	private Map<String,String> browseClassBooks = new HashMap<String,String>();
	private HTable browseBigclassBookTable;//浏览 大类热书表
	private Map<String,String> browseBigclassBooks = new HashMap<String,String>();
	
	private HTable readAuthorBookTable;// 阅读作者热书表
	private Map<String,String> readAuthorBooks = new HashMap<String,String>();
	private HTable readClassBookTable;// 阅读分类热书表
	private Map<String,String> readClassBooks = new HashMap<String,String>();
	private HTable readBigclassBookTable;//阅读大类热书表
	private Map<String,String> readBigclassBooks = new HashMap<String,String>();
	
	private HTable orderAuthorBookTable;// 订购作者热书表
	private Map<String,String> orderAuthorBooks = new HashMap<String,String>();
	private HTable orderClassBookTable;// 订购分类热书表
	private Map<String,String>orderClassBooks = new HashMap<String,String>();
	private HTable orderBigclassBookTable;//订购大类热书表
	private Map<String,String> orderBigclassBooks = new HashMap<String,String>();
	
	private HTable freeBookTable;//免费图书表
	private Set<String> freeBooks =new HashSet<String>();
	
	private HTable  bookstatusTable;//在架状态图书表
	private HashSet<String> bookstatusInfoMap = new HashSet<String>();//bookstatus里取出的在架图书表
	
	private ConcurrentHashMap<String, BookSeriesInfo> bookInfoMap = new ConcurrentHashMap<String, BookSeriesInfo>();
	private Map<String,Integer> bookSeriesMap = new HashMap<String,Integer>();
	private Map<Integer, Set<String>> seriesBookSetMap = new HashMap<Integer,Set<String>>();
	
	private HTable frameBooksTable;// 在架图书表
	private static final byte[] REPOCF = Bytes.toBytes("cf");
	private static final byte[] REPOCQINFO = Bytes.toBytes("info");
	private static final byte[] REPOCQSERIESID = Bytes.toBytes("seriesid");
	private static final byte[] REPOCQORDERID = Bytes.toBytes("orderid");

	//private static final byte[] REPOCQCLASSID = Bytes.toBytes("classid");
	private static final byte[] REPOCQBIGCLASSID = Bytes.toBytes("bigclassid");
	
	private HTable fillerResultTable;//补白结果表
	private HTable orderTable;  // order also order
	private HTable readTable;
	private HTable browseTable;
	
	private static final byte[] REPOCQRESULT = Bytes.toBytes("result");
	private static final byte[] REPOCQBOOK = Bytes.toBytes("book");
	// 定时器
	private transient Thread loader = null;
	private static int hour = 0;
	private static float floatingRatio = 0; // 浮动比例
	private boolean isFirstLoad = true;
	
	private String addrs[] = null;

	private ConcurrentHashMap<String, HashMap<String, Node>> trees = new ConcurrentHashMap<String, HashMap<String, Node>>();//存储树模型
	private HTable treesModel;//存储树节点
	private int treesNumber = 0;
	

	
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	/*   prepare()做了以下工作：
	 *   1.初始化参数
	 *   2.初始化hbase，初始补白结果表、免费图书表、作者热书表、分类热书表、大类热书表，图书黑名单表，在架图书表，
	 *     订购还订购关联推荐结果表，阅读还阅读关联推荐结果表，浏览还浏览关联推荐结果表
	 *   3.每天定时读取 在架图书信息，黑名单信息，及免费图书信息
	 *   4.redis初始化
	*/  
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		//PrintHelper.print("yueqian FilterBolt prepare() begin.");
		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());

		// service related configuration
		resultExpireTime = Integer.parseInt(appConf.getParam("result_expire_time"));
		resultTable = appConf.getParam("result_table");
		recNumber = Integer.parseInt(appConf.getParam("rec_num"));
		orderRecNumber = Integer.parseInt(appConf.getParam("order_rec_num"));
		readRecNumber = Integer.parseInt(appConf.getParam("read_rec_num"));
		orderType = appConf.getParam("order_type");
		readType = appConf.getParam("read_type");
		browseType = appConf.getParam("browse_type");

		hour = Integer.parseInt((appConf.getParam("hour")));

		floatingRatio = Float.parseFloat((appConf.getParam("float_ratio")));
		treesNumber = Integer.parseInt((appConf.getParam("trees_num")));

		// hbase initialization
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",appConf.getParam("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort",appConf.getParam("hbase.zookeeper.property.clientPort"));
		
		//用户统计信息表
		while (null == userStatisticsInfoTable) {
			try {
				userStatisticsInfoTable = new HTable(conf, appConf.getParam("user_statistics_info"));
			} catch (IOException e) {
				PrintHelper.print("get userStatisticsInfoTable table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		//图书特征信息表
		while (null == bookFeatureInfoTable) {
			try {
				bookFeatureInfoTable = new HTable(conf, appConf.getParam("correlationrec_bookinfo"));
			} catch (IOException e) {
				PrintHelper.print("get bookFeatureInfoTable table instance error. try again...");
				e.printStackTrace();
			}
		}		

		//图书相似度信息表
		while (null == bookCorrelationSimTable) {
			try {
				bookCorrelationSimTable = new HTable(conf, appConf.getParam("correlationrec_bookcorrelation"));
			} catch (IOException e) {
				PrintHelper.print("get bookCorrelationSimTable table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		while (null == hTable) {
			try {
				hTable = new HTable(conf, appConf.getParam("user_read_history"));
			} catch (IOException e) {
				PrintHelper.print("get user_read_history table instance error. try again...");
				e.printStackTrace();
			}
		}

		// 补白结果表
		while (fillerResultTable == null) {
			try {
				fillerResultTable = new HTable(conf,appConf.getParam("coleration_rec_filler_result"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_filler_result table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 免费图书表
		while (freeBookTable == null) {
			try {
				freeBookTable = new HTable(conf,appConf.getParam("corelation_rec_free_book"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_free_book table instance error. try again...");
				e.printStackTrace();
			}
		}
		// 全部图书在架状态表
		while (bookstatusTable == null) {
			try {
				bookstatusTable = new HTable(conf,appConf.getParam("book_status"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get book_status table instance error. try again...");
				e.printStackTrace();
			}
		}
		//分类大类对应表
		while (classBigclassTable == null) {
			try {
				classBigclassTable = new HTable(conf,appConf.getParam("coleration_rec_class_bigclass"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_class_bigclass table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 浏览分类热书表
		while (browseClassBookTable == null) {
			try {
				browseClassBookTable = new HTable(conf,appConf.getParam("coleration_rec_browse_class_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_browse_class_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 浏览大类热书表
		while (browseBigclassBookTable == null) {
			try {
				browseBigclassBookTable = new HTable(conf,appConf.getParam("coleration_rec_browse_bigclass_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_browse_bigclass_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读作者热书表
		while (readAuthorBookTable == null) {
			try {
				readAuthorBookTable = new HTable(conf,appConf.getParam("coleration_rec_read_author_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_read_author_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读分类热书表
		while (readClassBookTable == null) {
			try {
				readClassBookTable = new HTable(conf,appConf.getParam("coleration_rec_read_class_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_read_class_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读大类热书表
		while (readBigclassBookTable == null) {
			try {
				readBigclassBookTable = new HTable(conf,appConf.getParam("coleration_rec_read_bigclass_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_read_bigclass_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购作者热书表
		while (orderAuthorBookTable == null) {
			try {
				orderAuthorBookTable = new HTable(conf,appConf.getParam("coleration_rec_order_author_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_order_author_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购分类热书表
		while (orderClassBookTable == null) {
			try {
				orderClassBookTable = new HTable(conf,appConf.getParam("coleration_rec_order_class_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_order_class_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购大类热书表
		while (orderBigclassBookTable == null) {
			try {
				orderBigclassBookTable = new HTable(conf,appConf.getParam("coleration_rec_order_bigclass_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("get coleration_rec_order_bigclass_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 图书黑名单表
		while (bookBlackListTable == null) {
			try {
				bookBlackListTable = new HTable(conf,appConf.getParam("unify_book_blacklist"));
			} catch (Exception e) {
				PrintHelper.print("get unify_book_blacklist table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 在架图书表
		while (frameBooksTable == null) {
			try {
				frameBooksTable = new HTable(conf,appConf.getParam("unify_frame_books"));
			} catch (Exception e) {
				PrintHelper.print("get unify_frame_books table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购还订购关联推荐结果数据
		while (orderTable == null) {
			try {
				orderTable = new HTable(conf,appConf.getParam("association_order"));
			} catch (IOException e) {
				PrintHelper.print("get association_order table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读还阅读关联推荐结果数据
		while (readTable == null) {
			try {
				readTable = new HTable(conf,appConf.getParam("association_read"));
			} catch (IOException e) {
				PrintHelper.print("get association_read table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 浏览还浏览推荐结果数据
		while (browseTable == null) {
			try {
				browseTable = new HTable(conf,appConf.getParam("association_browse"));
			} catch (IOException e) {
				PrintHelper.print("get association_browse table instance error. try again...");
				e.printStackTrace();
			}
		}
		// 加载树模型
		while (treesModel == null) {
			try {
				treesModel = new HTable(conf,appConf.getParam("correlationrec_models"));
			} catch (IOException e) {
				PrintHelper.print("get correlationrec_models table instance error. try again...");
				e.printStackTrace();
			}
		}		
      
    	// 定时器，每天定时从HBase上读取图书信息、黑名单
 		if (loader == null) {
 			loader = new Thread(new Runnable() {
 				public void run() {
 					while (true) {
 						long begin = System.currentTimeMillis();
 						// 加载图书信息
 						bookInfoMap = loadBookInfo();
 						bookstatusInfoMap.clear();
 						bookstatusInfoMap = loadBookStatusInfo();
 						//加载图书特征信息
// 						bookidAndLowvalMap.clear();
 						bookidAndLowvalMap = loadBookFeatureInfo();
// 						//加载图书id和雅俗值信息
// 						bookidAndLowvalueMap.clear();
// 						bookidAndLowvalueMap = loadBookLowValue();
// 						//加载图书相似度信息
// 						bookCorrelationSimMap.clear();
 						//bookCorrelationSimMap = loadBookCorrelationSim();
 						//加载免费图书信息
 						loadFreeBooks();
 						//加载补白需要的表
 						loadFillerData();
 						// 加载图书黑名单
 						loadBlackBooks();
 						//加载树模型
 						loadTrees();
 						
 						long end = System.currentTimeMillis();
 						PrintHelper.print("load frame books cost time :"+ (end - begin) + " ms");
 						long sleepTime = TimeUtil.getMillisFromNowToTwelveOclock(hour);
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
	
		addrs = appConf.getParam("input_redis").split("::");
		inputJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		inputJedis.auth(addrs[2]);
		while (null == inputJedis) {
			PrintHelper.print("get input jedis instance error. try again...");
			inputJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
			inputJedis.auth(addrs[2]);
		}
	
		addrs = appConf.getParam("output_redis").split("::");
		outputJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		outputJedis.auth(addrs[2]);
		while (null == outputJedis) {
			PrintHelper.print("get output jedis instance error. try again...");
			outputJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
			outputJedis.auth(addrs[2]);
		}
	 
		PrintHelper.print("yueqian FilterBolt prepare() end.");

	}


	/**************************************************************************************************************/
	/**************************************************************************************************************/
	/*   execute()做了以下工作：
	 *   1.拿到流里来的用户id、图书id、接口类型
	 *   2.获取该用户阅读历史、获取关联推荐结果、过滤用户历史、过滤同系列
	 *   3.将推荐结果写入redis
	*/ 
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//PrintHelper.print("---newReadFilterBolt execute() begin---");
		long newReadFilterBoltStart = System.currentTimeMillis();
		String userId = input.getStringByField("user");
		String bookId = input.getStringByField("book");
		String type = input.getStringByField("type");
		String authorId = input.getStringByField("author");
		String classId = input.getStringByField("classid");

		//PrintHelper.print("yueqian receive tuple: [" + userId + "," + bookId + type + "]");

		// 获取分类id、大类id
		String bigclassId ="-1";
		if (classId!=null&&!classId.equals("-1")&&!classId.isEmpty()) {
			Get get = new Get(Bytes.toBytes(classId));
			Result result = null;
			try {
				result = classBigclassTable.get(get);
				//classId= Bytes.toString(result.getValue(REPOCF, REPOCQCLASSID));
				bigclassId = Bytes.toString(result.getValue(REPOCF, REPOCQBIGCLASSID));
				//PrintHelper.print("yueqian get: ["+bookId+":"+ classId + "," + bigclassId +"]");
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			classId = "-1";
		}
		
		List<String> hisBooks = new ArrayList<String>();
		// 获取用户阅读历史
		if (userId.length() >= 4) {
			String rowKey = userId.substring(userId.length() - 4,userId.length() - 2)+ userId;
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = null;
			try {
				result = hTable.get(get);
				if (!result.isEmpty()) {// 有些用户，没有历史。
					List<Cell> cells = result.listCells();
					if (cells != null) {
						for (Cell cell : cells) {
							hisBooks.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// PrintHelper.print("yueqian" +" [" + userId + "] History books size : "+ hisBooks.size());

		//处理主流程
		StringBuffer sb = new StringBuffer();
		if ( readType.equals(type)) {
			
			//计算推荐结果【如果之前推荐结果有则查询并返回，如果没有则查询补白表，仍然没有则进行补白。（含过滤用户历史）】
			Map<String, String> books = compute(hisBooks, userId,bookId, type , authorId , classId , bigclassId);
			
			//过滤同系列图书
			//PrintHelper.print("rec result before filter series: "+ books.toString());
			books = filterSeriesBooks(books);
			//PrintHelper.print("rec result after filter series: "+ books.toString());
			
			Iterator<Entry<String, String>> iter = books.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				sb.append(entry.getKey() + "," + entry.getValue() + "|");
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
		} else {
			// for to do
		}
		
		//最终推荐结果写入redis
		try {
			outputJedis.setex(resultTable + ":" + userId + ":" + bookId + ":"+ type, resultExpireTime, sb.toString());
		} catch (JedisConnectionException e) {
			PrintHelper.print("JedisConnectionException[setex] is catched");
			e.printStackTrace();
			if (!outputJedis.isConnected()) {
				try {
					outputJedis.connect();
					outputJedis.setex(resultTable + ":" + userId + ":" + bookId + ":"+ type, resultExpireTime, sb.toString());
				} catch (JedisConnectionException e1) {
					PrintHelper.print("JedisConnectionException[connect] is catched");
					e1.printStackTrace();
				}
			}
		}
		PrintHelper.print("---newReadFilterBolt compute time(ms)---:"+userId+"|"+bookId+"|"+String.valueOf(System.currentTimeMillis()-newReadFilterBoltStart));
		//PrintHelper.print("yueqian recommend result for [" + userId + "," + bookId+ "," + type + "] : " + sb.toString());
	}
	
	
	/**********************************************其他一些函数*****************************************************/
	
	/********************************1.载入图书信息函数 loadBookInfo()   *******************************************/
	/********************************2.载入黑名单函数 loadBookInfo() ***********************************************/
	/********************************3.计算所有图书推荐结果函数compute()********************************************/
	/********************************4.补白推荐结果函数filler()【被compute()调用】**********************************/
	/********************************5.订购推荐结果过滤阅读推荐top20函数orderFillerRead()***************************/
	/********************************6.同分类/作者/大类补白函数classFiller()【被filler()调用】**********************/
	/********************************7.过滤同系列函数filterSeriesBooks()*******************************************/
	
	/**************************************************************************************************************/
	
	//加载图书信息 
	private ConcurrentHashMap<String, BookSeriesInfo> loadBookInfo() {
		
		ConcurrentHashMap<String, BookSeriesInfo> tmpBookInfoMap = new ConcurrentHashMap<String, BookSeriesInfo>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化图书系列号、顺序号
		ResultScanner rs;
		try {
			rs = frameBooksTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
			
				String seriesStr = Bytes.toString(result.getValue(REPOCF,REPOCQSERIESID));
				String orderStr = Bytes.toString(result.getValue(REPOCF,REPOCQORDERID));
				
				try {
					if (seriesStr == null || "".equals(seriesStr)||"-1".equals(seriesStr)) {
						seriesStr = "0";
					}
					if (orderStr == null || "".equals(orderStr)||"999999".equals(orderStr)) {
						orderStr = "0";
					}
					
					int seriesId = Integer.parseInt(seriesStr);
					int orderId = Integer.parseInt(orderStr);
					tmpBookInfoMap.put(bookId, new BookSeriesInfo(bookId,seriesId,orderId));
					
					if(seriesId!=0){
						bookSeriesMap.put(bookId, seriesId);
						if(seriesBookSetMap.containsKey(seriesId)){
							seriesBookSetMap.get(seriesId).add(bookId);
						}else{
							Set<String> bookSet = new HashSet<String>();
							bookSet.add(bookId);
							seriesBookSetMap.put(seriesId, bookSet);
						}
					}		
				} catch (Exception e) {
					PrintHelper.print("parse book info fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		//PrintHelper.print("Get " + tmpBookInfoMap.size()+ " frame books from hbase");
		int tmpSize = tmpBookInfoMap.size();
		int bookInfoSize = bookInfoMap.size();
		//PrintHelper.print("yueqian this time frame books :" + tmpSize+ ", last time frame books :" + bookInfoSize);

		if (isFirstLoad) { // 是否是第一次加载
			isFirstLoad = false;
			return tmpBookInfoMap;
		}

		if (Math.abs(tmpSize - bookInfoSize) / (float) bookInfoSize > floatingRatio) {
			PrintHelper.print("LOAD FRAME BOOKS FLOATING IS GREATER THAN "
					+ floatingRatio * 100 + "%");
			return bookInfoMap;
		}
		return tmpBookInfoMap;
	}
	
	//加载图书信息 
	private HashSet<String> loadBookStatusInfo() {
		
		HashSet<String> tmpBookInfoMap = new HashSet<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化在架表
		ResultScanner rs;
		tmpBookInfoMap.clear();
		try {
			rs = bookstatusTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String statusStr = Bytes.toString(result.getValue(REPOCF,REPOCQRESULT));
				try {
					//全部书加载
					if(statusStr.equals("13")){
						tmpBookInfoMap.add(bookId);
					}
					
				} catch (Exception e) {
					PrintHelper.print("parse book info fail...");
					e.printStackTrace();
					continue;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		//PrintHelper.print("Get " + tmpBookInfoMap.size()+ "  frame books from hbase");
		int tmpSize = tmpBookInfoMap.size();
		//int bookInfoSize = bookstatusInfoMap.size();
		PrintHelper.print("yueqian this time frame books :" + tmpSize+ ", last time frame books :");
		return tmpBookInfoMap;
	}
	//加载免费图书表
	private void loadFreeBooks() {
		freeBooks.clear();
		Scan scan = new Scan();
		scan.setCaching(100);
		Get get = new Get(Bytes.toBytes("freebook"));
		Result result = null;
		String freebookStr = null;
		try {
			result = freeBookTable.get(get);
			freebookStr= Bytes.toString(result.getValue(REPOCF, REPOCQBOOK));
			String [] fields = freebookStr.split("\\|");
			for(int i=0;i<fields.length;i++){
				freeBooks.add(fields[i]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + freeBooks.size()+ " free books from hbase");
	}
	
	//加载图书黑名单
	private void loadBlackBooks() {
		blackBooks.clear();
		Scan scan = new Scan();
		scan.setCaching(100);
		try {
			ResultScanner rs = bookBlackListTable.getScanner(scan);
			for (Result result : rs) {
				String book = Bytes.toString(result.getRow());
				blackBooks.add(book);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + blackBooks.size()+ " black books from hbase");
	}
	
	private void loadFillerData(){
		Scan scan = new Scan();
		scan.setCaching(100);
		//浏览分类/大类热书表加载
		browseClassBooks.clear();
		try {
			ResultScanner rs = browseClassBookTable.getScanner(scan);
			for (Result result : rs) {
				String c = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    browseClassBooks.put(c, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + browseClassBooks.size()+ " browse class books from hbase");
		browseBigclassBooks.clear();
		try {
			ResultScanner rs = browseBigclassBookTable.getScanner(scan);
			for (Result result : rs) {
				String bc = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    browseBigclassBooks.put(bc, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + browseBigclassBooks.size()+ "browse bigclass books from hbase");
		
		//阅读作者/分类/大类热书表加载
		readAuthorBooks.clear();
		try {
			ResultScanner rs = readAuthorBookTable.getScanner(scan);
			for (Result result : rs) {
				String author = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    readAuthorBooks.put(author, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + readAuthorBooks.size()+ "read author books from hbase");
		readClassBooks.clear();
		try {
			ResultScanner rs = readClassBookTable.getScanner(scan);
			for (Result result : rs) {
				String c = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    readClassBooks.put(c, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + readClassBooks.size()+ " read class books from hbase");
		readBigclassBooks.clear();
		try {
			ResultScanner rs = readBigclassBookTable.getScanner(scan);
			for (Result result : rs) {
				String bc = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    readBigclassBooks.put(bc, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + readBigclassBooks.size()+ "read bigclass books from hbase");
		
		//订购作者/分类/大类热书表加载
		orderAuthorBooks.clear();
		try {
			ResultScanner rs = orderAuthorBookTable.getScanner(scan);
			for (Result result : rs) {
				String author = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    orderAuthorBooks.put(author, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + orderAuthorBooks.size()+ " order author books from hbase");
		orderClassBooks.clear();
		try {
			ResultScanner rs = orderClassBookTable.getScanner(scan);
			for (Result result : rs) {
				String c = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    orderClassBooks.put(c, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + orderClassBooks.size()+ " order class books from hbase");
		orderBigclassBooks.clear();
		try {
			ResultScanner rs = orderBigclassBookTable.getScanner(scan);
			for (Result result : rs) {
				String bc = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    orderBigclassBooks.put(bc, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + orderBigclassBooks.size()+ " order bigclass books from hbase");
	}
	
	//计算推荐结果,，并过滤用户历史
	private Map<String, String> compute(List<String> historyBooks,String userId,String bookId, String type,String authorId,String classId,String bigclassId) {
		PrintHelper.print("---compute() begin---" + userId + "|" + bookId);
		List<String> offLineResult = new ArrayList<String>();
		// 获取推荐结果
		try {
			//先查离线推荐表
			Get get = new Get(Bytes.toBytes(bookId));
			Result result = null;
			String resultStr = null;
			if (orderType.equals(type)) { // 订购还订购
				result = orderTable.get(get);
				resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
			} else if (readType.equals(type)) {
				// 阅读还阅读
				//PrintHelper.print("--- readType start ---");
				
				long selRecResultTime = System.currentTimeMillis();
				result = readTable.get(get);
				String recResult = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
				String selRecResultCostTime = String.valueOf(System.currentTimeMillis()-selRecResultTime);
				
				long calculateCrossInfoTime = System.currentTimeMillis();
				String calculateCrossInfo = calculateCrossInfo(userId,bookId);// 返回数据格式:交叉信息;用户统计信息
				String calculateCrossInfoCostTime = System.currentTimeMillis()-calculateCrossInfoTime+"";
				
				if(calculateCrossInfo != null){
					
					String[] splitCrossInfo = calculateCrossInfo.split(";",-1);
					
					if(splitCrossInfo[0] == null || splitCrossInfo[1] == null  || recResult == null){
						resultStr = recResult;
					}else{
						String corssInfo = splitCrossInfo[0]; //用户交叉信息
						String userCountInfo = splitCrossInfo[1]; //用户统计信息
						
						List<String> splitRecResult = splitRecResult(recResult,bookId);
						Map<String, Float> forecastMap = new HashMap<String,Float>();
						
						NumberFormat instance = NumberFormat.getPercentInstance();
						instance.setMinimumFractionDigits(1);
						long allSelTime = System.currentTimeMillis();
						for(String a_b : splitRecResult){
							long fTime = System.currentTimeMillis();
							String selectSimResult = selectSimResult(a_b);
							//String selectSimResult = bookCorrelationSimMap.get(a_b);
							String selectSimResultTime = String.valueOf(System.currentTimeMillis()-fTime);
							if(selectSimResult == null){
								selectSimResult = "0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0";
							}
							String[] split = a_b.split("_",-1);
							String bookid2=split[1];
							String selectBookFeatureResult = bookidAndLowvalMap.get(bookid2);
							if(selectBookFeatureResult == null) continue;
							String selectBookFeatureResultTime = String.valueOf(System.currentTimeMillis()-fTime);
							
							String[] splitBookFeatureInfo = selectBookFeatureResult.split("\\|",-1);
							StringBuilder bookFeatureResult = new StringBuilder("");//图书特征信息
							for (int i = 3; i <= 15; i++) {
								bookFeatureResult.append(splitBookFeatureInfo[i] + "|");
							}
							String appendBookFeatureTime = String.valueOf(System.currentTimeMillis()-fTime);
							String finallyResult = userId + "|"+ bookId + "|" + bookid2 + "|" + bookFeatureResult.toString() + corssInfo + "|" + selectSimResult + "|" + userCountInfo;
							String finallyResultTime = String.valueOf(System.currentTimeMillis()-fTime);
							
							ArrayList<String> userData = new ArrayList<String>();
							Collections.addAll(userData, finallyResult.split("\\|"));
							String CollectionsTime = String.valueOf(System.currentTimeMillis()-fTime);
							double pre = 0;
							for (int i = 0; i < treesNumber; i++) {
								if (i == 0) pre += predict("0", "1", userData);
								else pre += predict(String.valueOf(i), "1", userData)*0.1;
							}
							String score = pre + "";
							String beforeTreeTime = String.valueOf(System.currentTimeMillis()-fTime);
							forecastMap.put(bookid2, Float.parseFloat(score));
							String treeTime = String.valueOf(System.currentTimeMillis()-fTime);
							
							long loopTime = System.currentTimeMillis()-fTime;
							if(loopTime >= 300){
								System.out.println("over time's info :"+ finallyResult + "|" + "selectSimResultTime is "+ selectSimResultTime + "|" +
														"selectBookFeatureResultTime is " +selectBookFeatureResultTime + "|" + "appendBookFeatureTime is " + appendBookFeatureTime + "|"+
										"finallyResultTime is "+finallyResultTime + "|" +"CollectionsTime is "+ CollectionsTime +"|"+ "beforeTreeTime is " + beforeTreeTime + "|"+"treeTime is "+ treeTime );
							}
						}
						String allSelCostTime = String.valueOf(System.currentTimeMillis()-allSelTime);
						Map<String, Float> sortMapByValue = sortMapByValue(forecastMap);
						StringBuilder sbResult = new StringBuilder("");
						for(Entry<String,Float> entry : sortMapByValue.entrySet()){
							String bookid = entry.getKey();
							float score = entry.getValue();
							sbResult.append(bookid+","+instance.format(score)+"|");
						}
						resultStr = sbResult.toString();
						//PrintHelper.print("---new-read-result---:" + resultStr);
						System.out.println("time count :"+ userId + "|"+ bookId + "|" + "selRecResultCostTime is "+selRecResultCostTime+"|"+"calculateCrossInfoCostTime is "+calculateCrossInfoCostTime+"|"+
								"allSelCostTime is "+ allSelCostTime);
					}
				}else{
					resultStr = recResult;
				}
			}else if (browseType.equals(type)) { // 浏览还浏览
				result = browseTable.get(get);
				resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
			}
			//离线推荐表没有结果时，查询补白表
			if(resultStr==null||resultStr.isEmpty()){
				//PrintHelper.print("yueqian Get " + bookId+ ","+ type+ " no result from corelation offline hbase table");
				Get getFiller = new Get(Bytes.toBytes(bookId));
				Result fillerResult = null;
				String fillerResultStr = null;
				fillerResult = fillerResultTable.get(getFiller);
				if(!fillerResult.isEmpty()){
					fillerResultStr = Bytes.toString(fillerResult.getValue(REPOCF, REPOCQRESULT));
					if (orderType.equals(type)) {// 订购还订购
						resultStr = fillerResultStr.split(";", -1)[2];
					} else if (readType.equals(type)) {// 阅读还阅读
						resultStr = fillerResultStr.split(";", -1)[1];
					} else if (browseType.equals(type)) { // 浏览还浏览
						resultStr = fillerResultStr.split(";", -1)[0];
					}
					//PrintHelper.print("yueqian Get " + bookId+ ","+ type+ " filler_result:"+ resultStr);
				}				
			}
			if (resultStr!=null&&!resultStr.isEmpty()) {
				String[] resultStrs = resultStr.split("\\|");  
				for (String res : resultStrs) {
				  String[] bookPercent = res.split(",", -1);
		          if (bookPercent.length == 2) {
		            offLineResult.add(bookPercent[0]);
		            offLineResult.add(bookPercent[1]);
		          }
				}
			}
		} catch (Exception e) {
			PrintHelper.print("[" + bookId + "] : Result from Hbase error ...");
			e.printStackTrace();
		}
		//PrintHelper.print("yueqian [" + bookId + "] : Result from Hbase : "+ offLineResult.toString());
		
		//有结果就过滤用户历史，没有结果则补白并过滤用户历史
		Map<String, String> selectedBookMap = new LinkedHashMap<String, String>();
		Map<String, String> backupBookMap = new LinkedHashMap<String, String>();
		//之前的没有推荐结果，则进行补白
		if (offLineResult.isEmpty()) {
			offLineResult = filler(bookId,type,authorId,classId,bigclassId);
		}
	   //过滤用户历史
		for (Iterator<String> iter = offLineResult.iterator(); iter.hasNext();) {
			if (selectedBookMap.size() >= readRecNumber) {
				break;
			}
			String recBookId = iter.next();
			String confidence = iter.next();
			if (blackBooks.contains(recBookId)) {
				continue;
			}
			if (!historyBooks.contains(recBookId)) {
				selectedBookMap.put(recBookId, confidence);
			} else {
				backupBookMap.put(recBookId, confidence);
			}
		}

		for (Entry<String, String> entry : backupBookMap.entrySet()) {
			if (selectedBookMap.size() >= readRecNumber) {
				break;
			}
			selectedBookMap.put(entry.getKey(), entry.getValue());
		}
		return selectedBookMap;
	}

	//补白
	private List<String> filler(String bookId, String type, String authorId ,String classId ,String bigclassId) {
		//long fillerTime = System.currentTimeMillis();
		List<String> computedFillerResult = new ArrayList<String>();
		List<String> FillerResult = new ArrayList<String>();
		List<String> browseComputedFillerResult = new ArrayList<String>();
		List<String> readComputedFillerResult = new ArrayList<String>();
		List<String> orderComputedFillerResult = new ArrayList<String>();
		
		Set<String> seriesBookSet = null;
		if(bookSeriesMap.containsKey(bookId)){
			String seriesId = Integer.toString(bookSeriesMap.get(bookId));
	    	seriesBookSet = seriesBookSetMap.get(seriesId);
		}
		//浏览还浏览补白【补同分类下的热书、不足补大类下的热书】
		computedFillerResult.clear();
		HashSet<String> picked = new HashSet<String>();
		picked.clear();
		computedFillerResult = classFiller(bookId,seriesBookSet,freeBooks,classId,browseClassBooks,readRecNumber,computedFillerResult,picked);
		browseComputedFillerResult = new ArrayList<String>(classFiller(bookId,seriesBookSet,freeBooks,bigclassId,browseBigclassBooks,readRecNumber,computedFillerResult,picked));
				
		//阅读还阅读补白【先补同作者下的热书、不足再补同分类下的热书、不足补大类下的热书】
		computedFillerResult.clear();
		picked.clear();
		if(!authorId.equals("0")){
			computedFillerResult = classFiller(bookId,seriesBookSet,freeBooks,authorId,readAuthorBooks,readRecNumber,computedFillerResult,picked);
		}	
			computedFillerResult = classFiller(bookId,seriesBookSet,freeBooks,classId,readClassBooks,readRecNumber,computedFillerResult,picked);
			readComputedFillerResult = new ArrayList<String>(classFiller(bookId,seriesBookSet,freeBooks,bigclassId,readBigclassBooks,readRecNumber,computedFillerResult,picked));
	
		
		//订购还订购补白【先补同作者下的热书、不足再补同分类下的热书、不足补大类下的热书】
		computedFillerResult.clear();
		picked.clear();
		if(!authorId.equals("0")){
			computedFillerResult = classFiller(bookId,seriesBookSet,freeBooks,authorId,orderAuthorBooks,orderRecNumber,computedFillerResult,picked);
		}
		computedFillerResult = classFiller(bookId,seriesBookSet,freeBooks,classId,orderClassBooks,orderRecNumber,computedFillerResult,picked);
		orderComputedFillerResult = new ArrayList<String>(classFiller(bookId,seriesBookSet,freeBooks,bigclassId,orderBigclassBooks,orderRecNumber,computedFillerResult,picked));
		
		//订购还订购过滤阅读还阅读top20
		orderComputedFillerResult = new ArrayList<String>(orderFillerRead(readComputedFillerResult,orderComputedFillerResult,orderRecNumber,readRecNumber));
		
		//结果写入hbase补白结果表
		StringBuffer sb = new StringBuffer();
	    if(!browseComputedFillerResult.isEmpty()&&!readComputedFillerResult.isEmpty()&&!orderComputedFillerResult.isEmpty()){
	    	for(int i=0;i<browseComputedFillerResult.size()-1;i=i+2){
				sb.append(browseComputedFillerResult.get(i)+",");
				sb.append(browseComputedFillerResult.get(i+1)+"|");
			}
			sb.deleteCharAt(sb.length()-1);
			sb.append(";");
			for(int i=0;i<readComputedFillerResult.size()-1;i=i+2){
				sb.append(readComputedFillerResult.get(i)+",");
				sb.append(readComputedFillerResult.get(i+1)+"|");
			}
			sb.deleteCharAt(sb.length()-1);
			sb.append(";");
			for(int i=0;i<orderComputedFillerResult.size()-1;i=i+2){
				sb.append(orderComputedFillerResult.get(i)+",");
				sb.append(orderComputedFillerResult.get(i+1)+"|");
			}
			sb.deleteCharAt(sb.length()-1);
			try {
				Put put = new Put(Bytes.toBytes(bookId));
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("result"),Bytes.toBytes(sb.toString()));
				fillerResultTable.put(put);
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
		
		
		//返回推荐结果
		if(type.equals(browseType)){
			FillerResult = new ArrayList<String>(browseComputedFillerResult);	
		}else if(type.equals(readType)){
			FillerResult = new ArrayList<String>(readComputedFillerResult);	
		}else if(type.equals(orderType)){
			FillerResult = new ArrayList<String>(orderComputedFillerResult);	
		}
		//System.out.println("--- Filler  time(ms)---:"+String.valueOf(System.currentTimeMillis()-fillerTime));
		return FillerResult;	
	}

    //订购过滤阅读top20
	private List<String> orderFillerRead(List<String> readComputedFillerResult,List<String> orderComputedFillerResult, 
			int orderRecNumber,int recNumber) {
		//PrintHelper.print("yueqian get: beforeorderComputedFillerResult[" +orderComputedFillerResult.size()+","+ orderComputedFillerResult.toString() +"]");
		//PrintHelper.print("yueqian get: readComputedFillerResult[" +readComputedFillerResult.size()+","+ readComputedFillerResult.toString() +"]");
		Set <String> readRecResult = new HashSet<String>();
		readRecResult.clear();
		
		for(int i=0;i<readComputedFillerResult.size()-1;i=i+2){
			readRecResult.add(readComputedFillerResult.get(i));
			if(i==2*(orderRecNumber-recNumber-1)){
				break;
			}
		}
		List<String> filtedOrderResult = new ArrayList<String>();
		String [] orderComputedFillerResultArray= orderComputedFillerResult.toArray(new String[orderComputedFillerResult.size()]);
		for(int j=0;j<orderComputedFillerResultArray.length-1;j=j+2){
			if(filtedOrderResult.size()==2*recNumber){
				break;
			}
			if(!readRecResult.contains(orderComputedFillerResultArray[j])){
				filtedOrderResult.add(orderComputedFillerResultArray[j]);
				filtedOrderResult.add(orderComputedFillerResultArray[j+1]);
			}
		}
		//PrintHelper.print("yueqian get: filtedOrderResult[" +filtedOrderResult.size()+","+ filtedOrderResult.toString() +"]");
		return filtedOrderResult;
	}

	//同分类/作者/大类补白
	private List<String> classFiller(String bookId, Set<String> seriesBookSet, Set<String> freeBookSet, String classId,
			Map<String, String> classBooks, int recNumber,List<String> computedFillerResult,Set<String> picked) {
		
		//PrintHelper.print("yueqian get: computedFillerResult["+classId+":" +computedFillerResult.size()+","+ computedFillerResult.toString() +"]");
		if (classId!=null&&!classId.isEmpty()&&!classId.equals("-1")&&classBooks!=null&&classBooks.containsKey(classId)) {
			
			String [] hotBook = classBooks.get(classId).split("\\|");
		
			ArrayList<String> hotBookExceptFree = new ArrayList<String>();
			for(int i=0;i<hotBook.length;i++){
				if(!freeBookSet.contains(hotBook[i]))
					hotBookExceptFree.add(hotBook[i]);
			}
			Collections.shuffle(hotBookExceptFree);
			//PrintHelper.print("yueqian get: hotbook[" +hotBook.length+","+ hotBook.toString() +"]");
			
			int sequence = computedFillerResult.size()/2;
	
			Iterator<String> it = hotBookExceptFree.iterator();
			while(it.hasNext()&&sequence<recNumber){
				String hotBookId = it.next();
				if(picked.contains(hotBookId)) continue;
				picked.add(hotBookId);
				if((seriesBookSet == null || !seriesBookSet.contains(hotBookId)) && !hotBookId.equals(bookId) && !freeBookSet.contains(hotBookId)) {
					computedFillerResult.add(hotBookId);
					computedFillerResult.add("");
					++sequence;	
				}				
			}		
		}
		//PrintHelper.print("yueqian get: fillerresult["+classId +computedFillerResult.size()+","+ computedFillerResult.toString() +"]");
		return computedFillerResult;
	}

	//过滤同系列图书
	private Map<String, String> filterSeriesBooks(Map<String, String> selectedBookMap) {
		long filterTime = System.currentTimeMillis();
		Map<Integer, Set<BookSeriesInfo>> serialBookMap = new HashMap<Integer, Set<BookSeriesInfo>>();
		Set<String> bookSet = selectedBookMap.keySet();
		for (String book : bookSet) {
			if (bookInfoMap.containsKey(book)) {
				BookSeriesInfo bookInfo = bookInfoMap.get(book);
				int seriesId = bookInfo.getSeriesId();
				int orderId = bookInfo.getOrderId();
				if (seriesId != 0) { // 系列图书
					boolean isMinOrder = true;
					if (serialBookMap.containsKey(seriesId)) {
						Set<BookSeriesInfo> seriesBooks = serialBookMap.get(seriesId);
						Iterator<BookSeriesInfo> itrator = seriesBooks.iterator();
						while (itrator.hasNext()) {
							BookSeriesInfo seriesBook = itrator.next();
							if (orderId < seriesBook.getOrderId()) {
								seriesBook.setMinOrder(false);
							} else {
								isMinOrder = false;
							}
						}
						BookSeriesInfo seriesBook = new BookSeriesInfo(book,seriesId, orderId, isMinOrder);
						seriesBooks.add(seriesBook);
						serialBookMap.put(seriesId, seriesBooks);
					} else {
						BookSeriesInfo seriesBook = new BookSeriesInfo(book,
								seriesId, orderId, true);
						Set<BookSeriesInfo> seriesBooks = new HashSet<BookSeriesInfo>();
						seriesBooks.add(seriesBook);
						serialBookMap.put(seriesId, seriesBooks);
					}
				}
			}
		}
		Set<Integer> serialBookSet = serialBookMap.keySet();
		Iterator<Integer> it = serialBookSet.iterator();
		while (it.hasNext()) {
			int seriesId = it.next();
			Set<BookSeriesInfo> seriesBookInfos = serialBookMap.get(seriesId);
			Iterator<BookSeriesInfo> itrator = seriesBookInfos.iterator();
			while (itrator.hasNext()) {
				BookSeriesInfo seriesBookInfo = itrator.next();
				String bookId = seriesBookInfo.getBookId();
				if (seriesBookInfo.isMinOrder()) {
					continue;
				}
				selectedBookMap.remove(bookId);
			}
		}
		Map<String, String> booksMap = new LinkedHashMap<String, String>();
		for (Entry<String, String> entry : selectedBookMap.entrySet()) {
			if (booksMap.size() >= recNumber) {
				break;
			}
			//PrintHelper.print("filter series: "+ entry.getKey());
			if(bookstatusInfoMap.contains(entry.getKey())){
				//PrintHelper.print("filter series: "+ entry.getKey());
				booksMap.put(entry.getKey(), entry.getValue());
			}
			
		}
		System.out.println("--- Filter time(ms)---:"+String.valueOf(System.currentTimeMillis()-filterTime));
		return booksMap;
	}
	
	//分割推荐结果
	private List<String> splitRecResult(String str,String bookid){
		
		List<String> list = new ArrayList<String>();
		String[] split = str.split("\\|",-1);
		for (int i = 0; i < split.length; i++) {
			String[] split2 = split[i].split(",");
			list.add(bookid + "_" + split2[0]);
		}
		//System.out.println("chenyuxiao   list.size() " + list.size());
		return list;
	}
	
	//查询相似度信息
	private String selectSimResult(String bookid){//bookid : bookA_bookB
		Get getBook = new Get(Bytes.toBytes(bookid));
		Result result = null;
		String bookSimInfo = null;
		try {
			 result = bookCorrelationSimTable.get(getBook);
			 bookSimInfo = Bytes.toString(result.getValue(REPOCF, REPOCQINFO));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return bookSimInfo;
	}
	
	//计算用户交叉信息
	private String calculateCrossInfo(String  userid,String bookid) throws IOException{
		Get get = new Get(Bytes.toBytes(userid));
		//Get getBook = new Get(Bytes.toBytes(bookid));
		Result result = null;
		String allInfo = null;
		String preferenceInfo = null;
		String readClassInfo = null;
		String countInfo = null;
		String interVector = null;  //交叉信息
		
		//Result bookResult = null;
		String bookAllInfo = null;
		Double low_value = null;
		try {
		    result = userStatisticsInfoTable.get(get);
		    allInfo = Bytes.toString(result.getValue(REPOCF, REPOCQINFO));//用户统计信息
		    
		    //bookResult = bookFeatureInfoTable.get(getBook);
		    //bookAllInfo = Bytes.toString(bookResult.getValue(REPOCF, REPOCQINFO));//图书特征信息
		    bookAllInfo = bookidAndLowvalMap.get(bookid);
		    
		    if(allInfo == null || bookAllInfo == null) return null;
		    String[] split = allInfo.split(";",-1);
		    //所需用户相关信息
		    if( split.length == 3  && allInfo!=null && bookAllInfo !=null){
		    	preferenceInfo = split[0];//偏好向量
		    	String[] splitPreferenceInfo = preferenceInfo.split("\\|");
                String msisdn = userid;
                String class_weight = splitPreferenceInfo[1];
                String new_weight = splitPreferenceInfo[2];
                String famous_weight = splitPreferenceInfo[3];
                String serialize_weight = splitPreferenceInfo[4];
                String charge_weight = splitPreferenceInfo[5];
                String sale_weight = splitPreferenceInfo[6];
                String pack_weight = splitPreferenceInfo[7];
                String man_weight = splitPreferenceInfo[8];
                String female_weight = splitPreferenceInfo[9];
                String low_weight = splitPreferenceInfo[10];
                String high_weight = splitPreferenceInfo[11];
                String hot_weight = splitPreferenceInfo[12];
                String class1_id = splitPreferenceInfo[13];
                String class2_id = splitPreferenceInfo[14];
                String class3_id = splitPreferenceInfo[15];
                String stubborn_weight = splitPreferenceInfo[16];
                HashSet<String> simClass = new HashSet<String>();
                for (int i = 17; i < splitPreferenceInfo.length; i++) {
                    String classes = splitPreferenceInfo[i].trim();
                    if (!classes.equals("")) {
                        simClass.add(classes);
                    }
                }  
                UserVector userVector = new UserVector(msisdn, class_weight, new_weight, famous_weight, serialize_weight, charge_weight, sale_weight, 
                		pack_weight, man_weight, female_weight, low_weight, high_weight, hot_weight, class1_id, class2_id, class3_id, stubborn_weight, simClass);
		    	readClassInfo = split[1];//阅读分类
		    	String[] splitClassInfo = readClassInfo.split("\\|",-1);
		    	HashSet<String> readClassSet = new HashSet<String>();
                for (int i = 0; i < splitClassInfo.length; i++) {
                	readClassSet.add(splitClassInfo[i]);
                }
		    	//所需图书信息
    		    String[] bookAllInfoAplit = bookAllInfo.split("\\|",-1);
    		    low_value = Double.parseDouble(bookAllInfoAplit[22]);
				String classId = bookAllInfoAplit[2]; //分类id
				String chargeType = bookAllInfoAplit[3].trim().equals("") ? "0" : bookAllInfoAplit[3]; //计费类型
				String ifFinish = bookAllInfoAplit[9].trim().equals("") ? "0" : bookAllInfoAplit[9]; //是否完本
				String sexId = bookAllInfoAplit[16].trim().equals("") ? "0" : bookAllInfoAplit[16]; 
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");//设置日期格式
                String dateRecord = (bookAllInfoAplit[21].trim().equals("")||bookAllInfoAplit[21].length()!=14) ? "00000000000000" : bookAllInfoAplit[21];
                Date d=new Date();
                boolean after = false;
				try {
					after = df.parse(dateRecord).after(new Date(d.getTime() - 14 * 24 * 60 * 60 * 1000));
				} catch (ParseException e) {
					e.printStackTrace();
				}
         	    String ifNew = after? "0" : "1";
         	    PersonalizedBookVector bv = new PersonalizedBookVector(low_value, bookid, classId, ifNew, chargeType, ifFinish, sexId);
         	    interVector = bv.getInterVector(userVector, readClassSet);
         	    countInfo = split[2];
		    }
		} catch (Exception e) {
			
		}
		return interVector+";"+countInfo;
	}	

	//加载图书特征信息 
	private ConcurrentHashMap<String,String> loadBookFeatureInfo(){
		bookidAndLowvalMap.clear();
		ConcurrentHashMap<String,String> bookFeatureInfoMap = new ConcurrentHashMap<String,String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化在架表
		ResultScanner rs;
		try {
			rs = bookFeatureInfoTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String bookInfo = Bytes.toString(result.getValue(REPOCF,REPOCQINFO));
				bookFeatureInfoMap.put(bookId, bookInfo);//key:bookid value:bookFeatureInfo
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//PrintHelper.print("this time frame books  loadBookFeatureInfo :" + bookFeatureInfoMap.size() + ", last time frame books :");
		//System.out.println("chenyuxiao  bookFeatureInfoMap "  + bookFeatureInfoMap.size()  );
		return bookFeatureInfoMap;
	}

	//加载图书特征信息用于存放bookid和lowValue 
	private HashMap<String,Double> loadBookLowValue(){
		HashMap<String,Double> bookLowValueMap = new HashMap<String,Double>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化在架表
		ResultScanner rs;
		bookLowValueMap.clear();
		try {
			rs = bookFeatureInfoTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String bookInfo = Bytes.toString(result.getValue(REPOCF,REPOCQINFO));
				String[] splitBookInfo = bookInfo.split("\\|",-1);
				bookLowValueMap.put(bookId, Double.parseDouble(splitBookInfo[22]));//key:bookid value:lowValue
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian this time frame books  loadBookLowValue :" + bookLowValueMap.size()+ ", last time frame books :");
		//System.out.println("chenyuxiao  bookLowValueMap "  + bookLowValueMap.size()  );
		return bookLowValueMap;
	}	
	
	//加载图书关联度信息表
	private Map<String,String> loadBookCorrelationSim(){
		Map<String,String> bookCorrSimMap = new HashMap<String,String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化在架表
		ResultScanner rs;
		try {
			rs = bookCorrelationSimTable.getScanner(scan);
			for (Result result : rs) {
				String bookA_B = Bytes.toString(result.getRow());
				String simInfo = Bytes.toString(result.getValue(REPOCF,REPOCQINFO));
				bookCorrSimMap.put(bookA_B, simInfo);//key:bookid value:相似度信息
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//PrintHelper.print("yueqian this time frame books loadBookCorrelationSim :" + bookCorrSimMap.size()+ ", last time frame books :");
		//System.out.println("chenyuxiao   bookCorrSimMap   "  + bookCorrSimMap.size());
		return bookCorrSimMap;
	}
	//查询关联图书的图书特征
	private String selectBookFeatureResult(String bookid2){
		StringBuilder bookFeatureResult = new StringBuilder("");//图书特征信息
		Result result = null;
		String resultStr = null;
		Get get = new Get(Bytes.toBytes(bookid2));
		try {
			
			result = bookFeatureInfoTable.get(get);
			resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQINFO));//图书特征info
			
			if(resultStr == null) return bookFeatureResult.toString();
			String[] splitBookFeatureInfo = resultStr.split("\\|",-1);
			for (int i = 3; i <= 15; i++) {
				bookFeatureResult.append(splitBookFeatureInfo[i] + "|");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bookFeatureResult.toString();
	}
	//按value排序
	public Map<String, Float> sortMapByValue(Map<String, Float> oriMap) {
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		if (oriMap != null && !oriMap.isEmpty()) {
			List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(oriMap.entrySet());
			Collections.sort(entryList,
					new Comparator<Map.Entry<String, Float>>() {
						public int compare(Entry<String, Float> entry1,
								Entry<String, Float> entry2) {
							float value1 = 0, value2 = 0;
							try {
								value1 = entry1.getValue();
								value2 = entry2.getValue();
							} catch (NumberFormatException e) {
								value1 = 0;
								value2 = 0;
							}
							if (value2 > value1) {
								return 1;
							} else if (value2 < value1) {
								return -1;
							} else if ((value2 - value1) < 0.001) {
								return 0;
							} else {
								return 0;
							}

						}
					});
			Iterator<Map.Entry<String, Float>> iter = entryList.iterator();
			Map.Entry<String, Float> tmpEntry = null;
			while (iter.hasNext()) {
				tmpEntry = iter.next();
				sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
			}
		}
		return sortedMap;
	}	
	
	//加载树模型
	private void loadTrees() {
		trees.clear();
		Scan scan = new Scan();
		ResultScanner rs = null;
		try {
			rs = treesModel.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (rs != null) {
			String node, treeId, nodeId;
			for (Result result : rs) {
				treeId = Bytes.toString(result.getRow()).split("_")[0].trim();
				nodeId = Bytes.toString(result.getRow()).split("_")[1].trim();
				node = Bytes.toString(result.getValue(REPOCF, REPOCQINFO));
				String[] fields = node.split(";");
				if (fields.length < 2) continue;
				if (fields.length > 3) {
					HashMap<String, Node> nodes = new HashMap<String, Node>();
					if (trees.containsKey(treeId)) {
						nodes = trees.get(treeId);
					}
					nodes.put(nodeId, new Node(treeId, nodeId, Double.valueOf(fields[0].trim()),
							Boolean.valueOf(fields[1].trim()), Integer.valueOf(fields[2].trim()),
							Double.valueOf(fields[3].trim()),
							fields[4].trim(), fields[5].trim()));
					trees.put(treeId, nodes);
				} else {
					HashMap<String, Node> nodes = new HashMap<String, Node>();
					if (trees.containsKey(treeId)) {
						nodes = trees.get(treeId);
					}
					nodes.put(nodeId, new Node(treeId, nodeId, Double.valueOf(fields[0].trim()),
							Boolean.valueOf(fields[1].trim())));
					trees.put(treeId, nodes);
				}
			}
		}
		//PrintHelper.print("---TreeModel size()--- :" + trees.size());
	}	
	
	private double predict(String treeId, String nodeId, ArrayList<String> userData) {
		
		if (trees.get(treeId).get(nodeId).isLeaf()) {
			return trees.get(treeId).get(nodeId).getPredict();
		} else {
			double data = userData.get(trees.get(treeId).get(nodeId).getFeature()+3).isEmpty() ? 0 : Double.valueOf(userData.get(trees.get(treeId).get(nodeId).getFeature()+3));
			if (data <= trees.get(treeId).get(nodeId).getThreshold())
				return predict(treeId, trees.get(treeId).get(nodeId).getLeftNodeId(), userData);
			else return predict(treeId, trees.get(treeId).get(nodeId).getRightNodeId(), userData);
		}
	}	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
