package org.mras.bookrec.unifiedinterface;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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



public class HandleBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	
	private HTableInterface historyTable;
	private HTableInterface tagPrefTable;
	private HTableInterface recPoolTable;
	private HTableInterface recRepoTable;
	
	

	private static final byte[] REPOCF = Bytes.toBytes("cf");
	private static final byte[] REPOCQTAG = Bytes.toBytes("tag");
	private static final byte[] REPOCQPAGE = Bytes.toBytes("page");
	
	
	
	private static final byte[] USERTAGCF = Bytes.toBytes("cf");
	private static final byte[] USERTAGCQTAGS = Bytes.toBytes("tags");
	
	private Map<String, RecRepoBookInfo> RepoBookMap = new HashMap<String, RecRepoBookInfo>();
 	private Map<Integer, ArrayList<String>>  editionRepoListMap = new HashMap<Integer, ArrayList<String>>();
	private Map<String, ArrayList<String>>  tagRepoListMap = new HashMap<String, ArrayList<String>>();
	
	

	private Jedis respJedis;
	private String respTable;
	
	
	private int upperLimit ;
	private int lowerLimit;
	private int interLimit;
	private int respExpireTime;
	

	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		PrintHelper.print("FilterBolt prepare() begin.");
		

		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
		
//		//service related configuration		
		respTable = appConf.getParam("response_table");
		upperLimit = Integer.parseInt(appConf.getParam("rec_upper_limit"));
		lowerLimit = Integer.parseInt(appConf.getParam("rec_lower_limit"));
		interLimit = Integer.parseInt(appConf.getParam("rec_inter_limit"));		
		respExpireTime = Integer.parseInt(appConf.getParam("resp_expire_time"));	
	
		String addrs[]  = appConf.getParam("resp_redis").split("::");
		respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		while (respJedis == null) {
			PrintHelper.print("get resp jedis instance error. try again...");
			respJedis = new Jedis(addrs[0], Integer.parseInt(addrs[1]));
		}		
		PrintHelper.print("FilterBolt prepare() end.");
		
		
		
		//hbase initialization
		Configuration conf = HBaseConfiguration.create();
    	conf.set("hbase.zookeeper.quorum", appConf.getParam("hbase.zookeeper.quorum"));
    	conf.set("hbase.zookeeper.property.clientPort", appConf.getParam("hbase.zookeeper.property.clientPort"));
    	HConnection conn = null;
    	while (conn == null) {
    		try {    		
    			conn = HConnectionManager.createConnection(conf);
    		} catch (IOException e1) {
    			e1.printStackTrace();
    		}		
		}

    	//用户阅读历史表
    	while (historyTable == null) {
    		try {
    			historyTable = conn.getTable("user_read_history");
    		} catch (IOException e) {
    			PrintHelper.print("get hbase table user_read_history instance error. try again...");  		
    			e.printStackTrace();
    		}			
		}
    
    	
    	while (tagPrefTable == null) {
    		try {
    			tagPrefTable = conn.getTable("realpub_custom_tag");
    		} catch (IOException e) {
    			PrintHelper.print("get hbase table realpub_custom_tag instance error. try again...");  		
    			e.printStackTrace();
    		}			
		}
    	
    	while (recPoolTable == null) {
    		try {
    			recPoolTable = conn.getTable("unified_rec_pool");
    		} catch (IOException e) {
    			PrintHelper.print("get hbase table unified_rec_pool instance error. try again...");  		
    			e.printStackTrace();
    		}			
		}    	
    	
    	while (recRepoTable == null) {
    		try {
    			recRepoTable = conn.getTable("unified_rec_rep");   	    	
    		} catch (IOException e) {
    			PrintHelper.print("get hbase table unified_rec_pool instance error. try again...");  		
    			e.printStackTrace();
    		}			
		}
    	
    	//图书ID|定制标签|版面集	
    	Scan scan = new Scan();
    	scan.setCaching(100);
    	try {
			ResultScanner rs = recRepoTable.getScanner(scan);
			for (Result result : rs) {
				String bookId = Bytes.toString(result.getRow());
				String tag = Bytes.toString(result.getValue(REPOCF, REPOCQTAG));
				if (tag == null) { tag = "";}
				String editions = Bytes.toString(result.getValue(REPOCF,REPOCQPAGE));
				if (editions == null) { editions = "";}
				RecRepoBookInfo repoBook = new RecRepoBookInfo(tag, editions);   
				RepoBookMap.put(bookId, repoBook);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
    	
		//是否全部提前计算完   
		Iterator<Entry<String, RecRepoBookInfo>> iter = RepoBookMap.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, RecRepoBookInfo> next = iter.next();
			String book = "," + next.getKey();//补白图书格式：  ,bookid
			RecRepoBookInfo bookInfo = next.getValue();			
			for( int edition : bookInfo.getEditions() ) {
				if (editionRepoListMap.containsKey(edition)) {
					editionRepoListMap.get(edition).add(book);
				} else {
					ArrayList<String> RepoList = new ArrayList<String>();
					RepoList.add(book);
					editionRepoListMap.put(edition, RepoList);
				}
			}
			String tag = bookInfo.getTag();
			if(tagRepoListMap.containsKey(tag)) {
				tagRepoListMap.get(tag).add(book);			
			} else {
				ArrayList<String> RepoList = new ArrayList<String>();
				RepoList.add(book);
				tagRepoListMap.put(tag, RepoList);
			}			
		}
    	

		
	}



	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		PrintHelper.print("FilterBolt execute() begin.");

		
		String userId = input.getStringByField("user");
		int editionId;
		try {
			editionId = Integer.parseInt(input.getStringByField("edition"));
		} catch (NumberFormatException e) {
			editionId = 7;
		}
		if(editionId < 1 || editionId >7) {
			editionId = 7;
		}
		PrintHelper.print("receive tuple: [" + userId + "," + editionId +  "]");
		
		//获取推荐池中该用户的推荐结果
		Get get = new Get(Bytes.toBytes(userId));
		Result result = null;
		try {
			result = recPoolTable.get(get);			
		} catch (IOException e) {
			e.printStackTrace();
		}		
		
		
		//Book1|Book2|Book3,
		//Book1,上架时间|Book2,上架时间|Book3，上架时间
		//Book1,标签名|Book2,标签名|Book3，标签名
		
		//prtype,bookid
		//prtype,bookid,标签名
		//prtype,bookid,上架时间
		ArrayList<String> userRecPoolResult = new ArrayList<String>();
		if (result != null && !result.isEmpty()) {			
			List<Cell> cells= result.listCells();
			if (cells != null) {
				for (Cell cell : cells) {
					String recType  = Bytes.toString(CellUtil.cloneQualifier(cell)); //########3搜索类型是int，看看插入的时候怎么插入的##############
					String recResult = Bytes.toString(CellUtil.cloneValue(cell));
					String[] books = recResult.split("\\|");
					for (String book : books) {
						userRecPoolResult.add(recType + "," + book );
					}					
				}			
			}
		}
		
		//#######test
		System.out.println("====the book in userRecPoolResult====");
		for (String string : userRecPoolResult) {
			System.out.println(string);
		}
		//###########test
		//获取用户标签偏好
		try {
			result = tagPrefTable.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		HashSet<String> tagPref = new HashSet<String>();
		if(result != null && !result.isEmpty()) {			
			String tagPrefStr = Bytes.toString(result.getValue(USERTAGCF, USERTAGCQTAGS));
			if (tagPrefStr != null) {
				String[] tags = tagPrefStr.split("\\|"); //format: 订制标签ID1|订制标签ID2|.......
				for (String tag : tags) {
					tagPref.add(tag);
				}				
			}			
		}
		
		//#######test
		System.out.println("====the tag pref in tagpref====");
		for (String string : tagPref) {
			System.out.println(string);
		}
		//###########test
		
		
		
		/*推荐池里面的数据，不会包含用户历史*/
		List<String> poolListWithSelectedEdition = new ArrayList<String>();
		List<String> poolListWithSelectedTag = new ArrayList<String>();
		for (String book : userRecPoolResult) {			
			RecRepoBookInfo bookInfo = RepoBookMap.get(book.split(",")[1]);
			if (bookInfo == null) continue;
			HashSet<Integer> editions = bookInfo.getEditions();
			for (Integer edition : editions) {
				if (edition == editionId) {
					poolListWithSelectedEdition.add(book);
				}
			}
			String tag = bookInfo.getTag();
			if (!tagPref.isEmpty()) {
				if (tagPref.contains(tag)) {
					poolListWithSelectedTag.add(book);
				}				
			}			
		}	
		
		//#######test
		System.out.println("====poolListWithSelectedEdition====");
		for (String string : poolListWithSelectedEdition) {
			System.out.println(string);
		}
		
		System.out.println("====poolListWithSelectedTag====");
		for (String string : poolListWithSelectedTag) {
			System.out.println(string);
		}
		//###########test
		
		
		/*
		 * 第一步：根据版面选择，从推荐池选定可以推荐的图书
		 * 第二步：根据偏好选择信息(定制标签最多选五个，没有的话就没有top10这一步了，就只有后面随机100本)，先在上述结果中筛选满足偏好条件的图书，随机排序取前10本：
		 * 1）若够10本，则这10本为TOP 10，然后所有可推荐图书随机排序，向前去重，若图书重复，保留排在前面的结果及推荐类型，再取90本，共100本，如果总共不足80本的话，补足为80本（随机从推荐库选取，保证版本偏好,并且去历史）；
		 * 2）若不够10本，则从-满足标签+版面ID要求的未阅读图书补足10本为TOP10之后，所有可推荐图书（HBase）随机排序，向前去重；，若图书重复，保留排在前面的结果及推荐类型；，再取90本，共100本。,如果总共不足80本，补白不足为80本（随机从推荐库选取，保证版本偏好,并且去历史）。,
		 */
		HashSet<String> selectBookId = new  HashSet<String>();
		ArrayList<String> selectBook = new ArrayList<String>();
		Random rand = new Random();
		
		
		if (!tagPref.isEmpty()) {//用户有标签偏好
			ArrayList<String>  poolIntersection = new ArrayList<String>(poolListWithSelectedEdition);
			poolIntersection.retainAll(poolListWithSelectedTag);		

			Set<String> poolIntersectionBookIdSet = new HashSet<String>();//求版面+标签偏好的交集里面有几个不同的bookId
			for (String book : poolIntersection) {
				String bookId = book.split(",")[1];
				if (!poolIntersectionBookIdSet.contains(bookId)) {
					poolIntersectionBookIdSet.add(bookId);
				}					
			}			
			if (poolIntersectionBookIdSet.size() > interLimit) {//随机选择十本				
				while (selectBookId.size() < interLimit) {
					if (poolIntersection.isEmpty()) {
						break;
					}
					String book = poolIntersection.get(rand.nextInt(poolIntersection.size()));
					String bookId = book.split(",")[1];
					if (!selectBookId.contains(bookId)) {
						selectBookId.add(bookId);
						selectBook.add(book);
					}				
				}				
			} else {//补白到十本
				for (String book : poolIntersection) {
					String bookId = book.split(",")[1];
					if (!selectBookId.contains(bookId)) {
						selectBookId.add(bookId);
						selectBook.add(book);						
					}					
				}
				if (selectBookId.size() < interLimit) {
					List<String> repoListWithSelectedEdition = computeRepoListWithSelectedEdition(userId, editionId);//该列表去掉了用户历史
					List<String> repoListWithSelectedTag = new ArrayList<String>();					
					for (String tag : tagPref) {
						if (tagRepoListMap.containsKey(tag)) {
							repoListWithSelectedTag.addAll(tagRepoListMap.get(tag));
						}
					}
					/*repoListWithSelectedEdition是去历史的，所以repoIntersection也是去历史的*/
					ArrayList<String>  repoIntersection = new ArrayList<String>(repoListWithSelectedEdition);
					repoIntersection.retainAll(repoListWithSelectedTag);
					while (selectBookId.size() < interLimit) {// 补白到十本	,此处认为推荐库中肯定有足够的图书补白。	
						if (repoIntersection.isEmpty()) {
							break;
						}
						String book = repoIntersection.get(rand.nextInt(repoIntersection.size()));
						String bookId = book.split(",")[1];
						if (!selectBookId.contains(bookId)) {
							selectBookId.add(bookId);
							selectBook.add(book);						
						}				
					}						
				}				
			}			
			//选取同版面的图书,保证最终结果最多100，最少补足到80本
			computeListWithSelectedEdition(poolListWithSelectedEdition, selectBookId, selectBook, upperLimit, lowerLimit, userId, editionId);
		
		} else {//没有偏好
			computeListWithSelectedEdition(poolListWithSelectedEdition, selectBookId, selectBook, upperLimit, lowerLimit, userId, editionId);			
		}		

		StringBuffer sb = new StringBuffer();
		for (String book : selectBook) {
			sb.append(book + "|");
		}
		sb.deleteCharAt(sb.length() -1 );	
		
		
		PrintHelper.print("recommend result for [" + userId + "," + editionId  + "] : " + sb.toString());
		if (userId.equals("13958080393") || userId.equals("13858038966")) {
			editionId = 1;
		}
		respJedis.setex(respTable + ":" + userId + ":" + editionId, respExpireTime, sb.toString());		
		PrintHelper.print("FilterBolt execute() end.");

	}
	
	
	
	private void computeListWithSelectedEdition(List<String> poolListWithSelectedEdition, Set<String> selectBookId, List<String> selectBook, int upper, int lower, String userId, int editionId) {
		Random rand = new Random();
		
		Set<String> poolEditionBookIdList = new HashSet<String>();		
		for (String book : poolListWithSelectedEdition) {
			String bookId = book.split(",")[1];
			if (!poolEditionBookIdList.contains(bookId)) {
				poolEditionBookIdList.add(bookId);
			}					
		}
		
		poolEditionBookIdList.removeAll(selectBookId);  //除掉符合偏好的。	
		
		if (poolEditionBookIdList.size() > upper - selectBookId.size() ) {/*随机选取*/
			while (selectBookId.size() < upper) {
				if (poolListWithSelectedEdition.isEmpty()) {
					break;
				}
				String book = poolListWithSelectedEdition.get(rand.nextInt(poolListWithSelectedEdition.size()));/*type,bookid,xxx*/
				String bookId = book.split(",")[1];
				if (!selectBookId.contains(bookId)) {
					selectBookId.add(bookId);
					selectBook.add(book);
				}				
			}			
		} else {
			for (String book : poolListWithSelectedEdition) {
				String bookId = book.split(",")[1];
				if (!selectBookId.contains(bookId)) {
					selectBookId.add(bookId);
					selectBook.add(book);						
				}					
			}
			if (selectBookId.size() < lower) {
				List<String> repoListWithSelectedEdition = computeRepoListWithSelectedEdition(userId, editionId);//该列表去掉了用户历史					
				while (selectBookId.size() < lower) {// 补白到十本	，注意去历史
					if (repoListWithSelectedEdition.isEmpty()) {
						break;
					}
					String book = repoListWithSelectedEdition.get(rand.nextInt(repoListWithSelectedEdition.size()));
					String bookId = book.split(",")[1];
					if (!selectBookId.contains(bookId)) {
						selectBookId.add(bookId);
						selectBook.add(book);						
					}				
				}					
			}				
		}
	}
	
	
	//求推荐库中相关的图书，以备补白使用
	private List<String>  computeRepoListWithSelectedEdition(String userId, int editionId) {		
		List<String> userHistory = getUserHistory(userId);
		List<String> repoListWithSelectedEdition = editionRepoListMap.get(editionId);
		Iterator<String> iter = repoListWithSelectedEdition.iterator();
		while (iter.hasNext()) {			
			String bookId = iter.next().split(",")[1];
			if (userHistory.contains(bookId)) {
				iter.remove();
			}		
		}		
		return repoListWithSelectedEdition;		
	}
	
	
	private List<String> getUserHistory(String userId) {
		//获取用户阅读历史
		List<String> hisBooks = new ArrayList<String>();
		if (userId.length() >= 4) {
			String rowKey = userId.substring(userId.length() - 4, userId.length() -2) + userId;			
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = null;
			try {
				result = historyTable.get(get);
			} catch (IOException e) {
				e.printStackTrace();
			}			
			if (result != null && !result.isEmpty()) {//有些用户，没有历史。
				List<Cell> cells = result.listCells();
				if (cells != null) {
					for (Cell cell : cells) {
						hisBooks.add(Bytes.toString(CellUtil.cloneQualifier(cell)));			
					}				
				}			
			}
		}
		return hisBooks;		
	}
	
	
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}
	
}
