package com.eb.bi.rs.mras.bookrec.correrecrealtimefilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;

public class ReadFilterBolt extends BaseBasicBolt {
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

		PrintHelper.print("yueqian FilterBolt prepare() begin.");
		
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

		// hbase initialization
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",appConf.getParam("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort",appConf.getParam("hbase.zookeeper.property.clientPort"));
		
		
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
 						long end = System.currentTimeMillis();
 						PrintHelper.print("load frame books cost time :"+ (end - begin) + " ms");
 						//加载免费图书信息
 						loadFreeBooks();
 						//加载补白需要的表
 						loadFillerData();
 						// 加载图书黑名单
 						loadBlackBooks();
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

		String userId = input.getStringByField("user");
		String bookId = input.getStringByField("book");
		String type = input.getStringByField("type");
		String authorId = input.getStringByField("author");
		String classId = input.getStringByField("classid");

		PrintHelper.print("yueqian receive tuple: [" + userId + "," + bookId + ","+ type +","+ authorId +","+ classId+"]");

		// 获取分类id、大类id
		String bigclassId ="-1";
		if (classId!=null&&!classId.equals("-1")&&!classId.isEmpty()) {
			Get get = new Get(Bytes.toBytes(classId));
			Result result = null;
			try {
				result = classBigclassTable.get(get);
				//classId= Bytes.toString(result.getValue(REPOCF, REPOCQCLASSID));
				bigclassId = Bytes.toString(result.getValue(REPOCF, REPOCQBIGCLASSID));
				PrintHelper.print("yueqian get: ["+bookId+":"+ classId + "," + bigclassId +"]");
				
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
		if (orderType.equals(type) || readType.equals(type)|| browseType.equals(type)) {
			
			//计算推荐结果【如果之前推荐结果有则查询并返回，如果没有则查询补白表，仍然没有则进行补白。（含过滤用户历史）】
			Map<String, String> books = compute(hisBooks, userId,bookId, type,authorId,classId,bigclassId);
			
			//过滤同系列图书
			PrintHelper.print("rec result before filter series: "+ books.toString());
			books = filterSeriesBooks(books);
			PrintHelper.print("rec result after filter series: "+ books.toString());
			
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
		PrintHelper.print("yueqian recommend result for [" + userId + "," + bookId+ "," + type + "] : " + sb.toString());
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

		PrintHelper.print("Get " + tmpBookInfoMap.size()+ " frame books from hbase");
		int tmpSize = tmpBookInfoMap.size();
		int bookInfoSize = bookInfoMap.size();
		PrintHelper.print("yueqian this time frame books :" + tmpSize+ ", last time frame books :" + bookInfoSize);

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

		PrintHelper.print("Get " + tmpBookInfoMap.size()+ " all frame books from hbase");
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
		List<String> offLineResult = new ArrayList<String>();
		// 获取推荐结果
		try {
			//先查离线推荐表
			Get get = new Get(Bytes.toBytes(bookId));
			Result result = null;
			String resultStr = null;
			if (orderType.equals(type)) {// 订购还订购
				result = orderTable.get(get);
				resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
			} else if (readType.equals(type)) {// 阅读还阅读
				result = readTable.get(get);
				resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
			} else if (browseType.equals(type)) { // 浏览还浏览
				result = browseTable.get(get);
				resultStr = Bytes.toString(result.getValue(REPOCF, REPOCQRESULT));
			}
			//离线推荐表没有结果时，查询补白表
			if(resultStr==null||resultStr.isEmpty()){
				PrintHelper.print("yueqian Get " + bookId+ ","+ type+ " no result from corelation offline hbase table");
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
					PrintHelper.print("yueqian Get " + bookId+ ","+ type+ " filler_result:"+ resultStr);
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
		
		PrintHelper.print("yueqian get: computedFillerResult["+classId+":" +computedFillerResult.size()+","+ computedFillerResult.toString() +"]");
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
			PrintHelper.print("filter series: "+ entry.getKey());
			if(bookstatusInfoMap.contains(entry.getKey())){
				PrintHelper.print("filter series: "+ entry.getKey());
				booksMap.put(entry.getKey(), entry.getValue());
			}
			
		}
		return booksMap;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
