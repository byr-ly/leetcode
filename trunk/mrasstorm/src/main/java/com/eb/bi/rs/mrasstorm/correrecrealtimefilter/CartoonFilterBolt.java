package com.eb.bi.rs.mrasstorm.correrecrealtimefilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Vector;

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
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mrasstorm.correrecrealtimefilter.PrintHelper;
import com.eb.bi.rs.mrasstorm.correrecrealtimefilter.TimeUtil;

public class CartoonFilterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	private Jedis inputJedis;
	private Jedis outputJedis;
	
	private int recNumber;
	private int orderRecNumber;
	private int readRecNumber;
	private int resultExpireTime;
	private String resultTable;
	private String orderType;
	private String readType;
	
	private HTable hTable; //用户历史表
	
	private HTable bookBlackListTable;// 图书黑名单表
	private Vector<String> blackBooks = new Vector<String>();
	
	private HTable authorBookTable;// 作者书集表
	private Map<String,String> authorBooks = new HashMap<String,String>();
	
	private HTable allBookTable;//漫画所有在架图书表
	private Map<String,String> allBooks = new HashMap<String,String>();

	private HTable  bookstatusTable;//在架状态图书表
	private HashSet<String> bookInfoMap = new HashSet<String>();//bookstatus里取出的在架图书表
	
	private HTable readClassBookTable;// 阅读分类热书表
	private Map<String,String> readClassBooks = new HashMap<String,String>();
	
	private HTable orderClassBookTable;// 订购分类热书表
	private Map<String,String>orderClassBooks = new HashMap<String,String>();
	
	private static final byte[] REPOCF = Bytes.toBytes("cf");

	private HTable fillerResultTable;//补白结果表
	private HTable orderTable;  // order also order
	private HTable readTable;

	private static final byte[] REPOCQRESULT = Bytes.toBytes("result");
	private static final byte[] REPOCQBOOK = Bytes.toBytes("book");
	// 定时器
	private transient Thread loader = null;
	private static int hour = 0;
	private static float floatingRatio = 0; // 浮动比例
	private boolean isFirstLoad = true;



	
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	/*   prepare()做了以下工作：
	 *   1.初始化参数
	 *   2.初始化hbase，初始补白结果表、全部_免费图书表、作者热书表、分类热书表、大类热书表，图书黑名单表，在架图书表，
	 *     订购还订购关联推荐结果表，阅读还阅读关联推荐结果表，浏览还浏览关联推荐结果表
	 *   3.每天定时读取 在架图书信息，黑名单信息，全部_免费图书信息，补白源数据
	 *   4.redis初始化
	*/  
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		PrintHelper.print("yueqian cartoon FilterBolt prepare() begin.");

		PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());

		// service related configuration
		resultExpireTime = Integer.parseInt(appConf.getParam("result_expire_time"));
		resultTable = appConf.getParam("result_table");
		recNumber = Integer.parseInt(appConf.getParam("rec_num"));
		orderRecNumber = Integer.parseInt(appConf.getParam("cartoon_order_rec_num"));
		readRecNumber = Integer.parseInt(appConf.getParam("read_rec_num"));
		orderType = appConf.getParam("order_type");
		readType = appConf.getParam("read_type");

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
				PrintHelper.print("cartoon get user_read_history table instance error. try again...");
				e.printStackTrace();
			}
		}

		// 补白结果表
		while (fillerResultTable == null) {
			try {
				fillerResultTable = new HTable(conf,appConf.getParam("coleration_rec_filler_result"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get coleration_rec_filler_result table instance error. try again...");
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
		// 全部在架图书表
		while (allBookTable == null) {
			try {
				allBookTable = new HTable(conf,appConf.getParam("coleration_rec_cartoon_all_books"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get coleration_rec_free_book table instance error. try again...");
				e.printStackTrace();
			}
		}
			
		// 作者书集表
		while (authorBookTable == null) {
			try {
				authorBookTable = new HTable(conf,appConf.getParam("coleration_rec_cartoon_author_books"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get coleration_rec_order_author_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读分类热书表
		while (readClassBookTable == null) {
			try {
				readClassBookTable = new HTable(conf,appConf.getParam("coleration_rec_read_class_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get coleration_rec_read_class_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购分类热书表
		while (orderClassBookTable == null) {
			try {
				orderClassBookTable = new HTable(conf,appConf.getParam("coleration_rec_order_class_hotbook"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get coleration_rec_order_class_hotbook table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 图书黑名单表
		while (bookBlackListTable == null) {
			try {
				bookBlackListTable = new HTable(conf,appConf.getParam("unify_book_blacklist"));
			} catch (Exception e) {
				PrintHelper.print("cartoon get unify_book_blacklist table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 订购还订购关联推荐结果数据
		while (orderTable == null) {
			try {
				orderTable = new HTable(conf,appConf.getParam("association_order"));
			} catch (IOException e) {
				PrintHelper.print("cartoon get association_order table instance error. try again...");
				e.printStackTrace();
			}
		}
		
		// 阅读还阅读关联推荐结果数据
		while (readTable == null) {
			try {
				readTable = new HTable(conf,appConf.getParam("association_read"));
			} catch (IOException e) {
				PrintHelper.print("cartoon get association_read table instance error. try again...");
				e.printStackTrace();
			}
		}
		
    	// 定时器，每天定时从HBase上读取图书信息、黑名单
 		if (loader == null) {
 			loader = new Thread(new Runnable() {
 				public void run() {
 					while (true) {
 						long begin = System.currentTimeMillis();
 						//加载在架图书信息
 						bookInfoMap = loadBookInfo();
 						long end = System.currentTimeMillis();
 						PrintHelper.print("cartoon load frame books cost time :"+ (end - begin) + " ms");
 						//加载图书黑名单
 						loadBlackBooks();
 						//加载补白源数据
 						loadFillerData();
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
		String addrs[] = null;
	
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
	 
		PrintHelper.print("yueqian cartoon FilterBolt prepare() end.");

	}


	/**************************************************************************************************************/
	/**************************************************************************************************************/
	/*   execute()做了以下工作：
	 *   1.拿到流里来的用户id、图书id、接口类型
	 *   2.获取该用户阅读历史、获取关联推荐结果（查询或者补白）、过滤用户历史
	 *   3.将推荐结果写入redis
	*/ 
	/**************************************************************************************************************/
	/**************************************************************************************************************/
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		PrintHelper.print("yueqian FilterBolt execute() begin.");

		String userId = input.getStringByField("user");
		String bookId = input.getStringByField("book");
		String type = input.getStringByField("type");
		String authorId = input.getStringByField("author");
		String classId = input.getStringByField("classid");

		PrintHelper.print("yueqian cartoon receive tuple: [" + userId + "," + bookId + ","+ type +","+ authorId +","+ classId+"]");

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
		PrintHelper.print("yueqian cartoon" +"[" + userId + "] History books size : "+ hisBooks.size());

		//处理主流程
		StringBuffer sb = new StringBuffer();
		if (orderType.equals(type) || readType.equals(type)) {
			
			//计算推荐结果【如果之前推荐结果有则查询并返回，如果没有则查询补白表，仍然没有则进行补白。（含过滤用户历史）】
			Map<String, String> books = compute(hisBooks,userId,bookId,type,authorId,classId);
			PrintHelper.print("yueqian [" + bookId+ "] After compute method==>books : " + books);

			Iterator<Entry<String, String>> iter = books.entrySet().iterator();
			int count =0;
			while (iter.hasNext()&&count<recNumber) {
				Entry<String, String> entry = iter.next();
				if(bookInfoMap.contains(entry.getKey())){
					sb.append(entry.getKey() + "," + entry.getValue() + "|");
					count++;
					PrintHelper.print("filter series: "+ count);
				}
			}
			if (sb.length() > 0) {
				sb.deleteCharAt(sb.length() - 1);
			}
		} else {
			// for to do
		}
		
		//最终推荐结果写入redis
		PrintHelper.print("yueqian cartoon recommend result for [" + userId + "," + bookId+ "," + type + "] : " + sb.toString());
		outputJedis.setex(resultTable + ":" + userId + ":" + bookId + ":"+ type, resultExpireTime, sb.toString());
		PrintHelper.print("yueqian cartoon FilterBolt execute() end.");
	}
	
	
	/**********************************************其他一些函数*****************************************************/
	/*1.loadBookInfo() 载入在架图书
	 *2.loadFreeBooks()  载入免费书表
	 *3.loadFillerData() 载入补白
	 *4.loadBlackBooks() 载入黑名单
	 *5.compute() 计算所有图书推荐结果
	 *6.filler() 补白推荐结果【被compute()调用】
	 *7.orderFillerRead()  订购过滤阅读top10
	 *8.classFiller() 同分类/作者/全部随机补白 【被filler()调用】
	 */

	//加载图书信息 
	private HashSet<String> loadBookInfo() {
		
		HashSet<String> tmpBookInfoMap = new HashSet<String>();
		Scan scan = new Scan();
		scan.setCaching(100);
		// 初始化在架表
		tmpBookInfoMap.clear();
		ResultScanner rs;
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

		PrintHelper.print("Get " + tmpBookInfoMap.size()+ "  all frame books from hbase");
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
	
	//加载补白源数据表
	private void loadFillerData() {
		Scan scan = new Scan();
		scan.setCaching(100);
				
		//全部书加载
		allBooks.clear();
		try {
			ResultScanner rs = allBookTable.getScanner(scan);
			for (Result result : rs) {
				String author = Bytes.toString(result.getRow());
				PrintHelper.print("yueqian Get " + author + "read all books  from hbase");
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    allBooks.put(author, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + allBooks.size() + "read all books  from hbase");
		//作者书集加载
		authorBooks.clear();
		try {
			ResultScanner rs = authorBookTable.getScanner(scan);
			for (Result result : rs) {
				String author = Bytes.toString(result.getRow());
				String hotbookStr = Bytes.toString(result.getValue(REPOCF,REPOCQBOOK));
			    authorBooks.put(author, hotbookStr);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		PrintHelper.print("yueqian Get " + authorBooks.size()+ "read author books from hbase");
		
		//阅读分类热书表加载
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
		
		//订购分类热书表加载
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
	
	//计算推荐结果,，并过滤用户历史
	private Map<String, String> compute(List<String> historyBooks,String userId,String bookId, String type,String authorId,String classId) {
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
						resultStr = fillerResultStr.split(";", -1)[1];
					} else if (readType.equals(type)) {// 阅读还阅读
						resultStr = fillerResultStr.split(";", -1)[0];
					} 
					PrintHelper.print("yueqian cartoon Get " + bookId+ ","+ type+ " filler_result:"+ resultStr);
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
		PrintHelper.print("yueqian cartoon [" + bookId + "] : Result from Hbase : "+ offLineResult.toString());
		
		//有结果就过滤用户历史，没有结果则补白并过滤用户历史
		Map<String, String> selectedBookMap = new LinkedHashMap<String, String>();
		Map<String, String> backupBookMap = new LinkedHashMap<String, String>();
		//之前的没有推荐结果，则进行补白
		if (offLineResult.isEmpty()) {
			
			offLineResult = filler(bookId,type,authorId,classId);
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
	private List<String> filler(String bookId, String type, String authorId ,String classId) {
		
		List<String> computedFillerResult = new ArrayList<String>();
		List<String> FillerResult = new ArrayList<String>();
		List<String> readComputedFillerResult = new ArrayList<String>();
		List<String> orderComputedFillerResult = new ArrayList<String>();

		//阅读还阅读补白【先补同作者下的书、不足再补同分类下的热书、不足补全部的书】
		computedFillerResult.clear();
		HashSet<String> picked = new HashSet<String>();
		picked.clear();
		if(!authorId.equals("0")){
			computedFillerResult = classFiller(bookId,authorId,authorBooks,readRecNumber,computedFillerResult,picked);
		}	
			computedFillerResult = classFiller(bookId,classId,readClassBooks,readRecNumber,computedFillerResult,picked);
			readComputedFillerResult = new ArrayList<String>(classFiller(bookId,"allbook",allBooks,readRecNumber,computedFillerResult,picked));
	
		
		//订购还订购补白【先补同作者下的热书、不足再补同分类下的热书、不足补大类下的热书】
		computedFillerResult.clear();
		picked.clear();
		if(!authorId.equals("0")){
			computedFillerResult = classFiller(bookId,authorId,authorBooks,orderRecNumber,computedFillerResult,picked);
		}
		computedFillerResult = classFiller(bookId,classId,orderClassBooks,orderRecNumber,computedFillerResult,picked);
		orderComputedFillerResult = new ArrayList<String>(classFiller(bookId,"allbook",allBooks,orderRecNumber,computedFillerResult,picked));
		
		//订购还订购过滤阅读还阅读top10
		orderComputedFillerResult = new ArrayList<String>(orderFillerRead(readComputedFillerResult,orderComputedFillerResult,orderRecNumber,readRecNumber));
		
		//结果写入hbase补白结果表
		StringBuffer sb = new StringBuffer();
	    if(!readComputedFillerResult.isEmpty()&&!orderComputedFillerResult.isEmpty()){
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
		if(type.equals(readType)){
			FillerResult = new ArrayList<String>(readComputedFillerResult);	
		}else if(type.equals(orderType)){
			FillerResult = new ArrayList<String>(orderComputedFillerResult);	
		}
		return FillerResult;	
	}

    //订购过滤阅读top10
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
    
	//同作者/分类/全部随机补白
	private List<String> classFiller(String bookId, String classId,
			Map<String, String> classBooks, int recNumber,List<String> computedFillerResult,Set<String> picked) {
		
		PrintHelper.print("yueqian get: computedFillerResult["+classId+":" +computedFillerResult.size()+","+ computedFillerResult.toString() +"]");
		if (classId!=null&&!classId.isEmpty()&&!classId.equals("-1")&&classBooks!=null&&classBooks.containsKey(classId)) {
			
			String [] hotBook = classBooks.get(classId).split("\\|");
			
			ArrayList<String> hotBookExceptFree = new ArrayList<String>();
			for(int i=0;i<hotBook.length;i++){
				hotBookExceptFree.add(hotBook[i]);
			}
			Collections.shuffle(hotBookExceptFree);
			
			int sequence = computedFillerResult.size()/2;
			//PrintHelper.print("yueqian get: hotbook[" +hotBook.length+","+ hotBook.toString() +"]");
			Iterator<String> it = hotBookExceptFree.iterator();
			while(it.hasNext()&&sequence<recNumber){
				String hotBookId = it.next();
				if(picked.contains(hotBookId)) continue;
				picked.add(hotBookId);
				computedFillerResult.add(hotBookId);
				computedFillerResult.add("");
				++sequence;	
			}						
		}
		//PrintHelper.print("yueqian get: fillerresult["+classId +computedFillerResult.size()+","+ computedFillerResult.toString() +"]");
		return computedFillerResult;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
