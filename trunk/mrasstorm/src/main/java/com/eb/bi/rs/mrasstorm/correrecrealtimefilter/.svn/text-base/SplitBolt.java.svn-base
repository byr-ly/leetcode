package com.eb.bi.rs.mrasstorm.correrecrealtimefilter;

import java.util.HashSet;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.eb.bi.rs.frame.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame.common.storm.config.PluginConfig;
import com.eb.bi.rs.mrasstorm.correrecrealtimefilter.PrintHelper;

public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private String allPhoneNumber;
	private HashSet<String> phoneNumber = new HashSet<String>();
	private String[] limit;
	private HashSet<String> cartoon_classid = new HashSet<String>();
	private int digit;
	private String testData;
	private int digitNum;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		PluginConfig appConf = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
		digit = Integer.parseInt(appConf.getParam("digit_data"));//测试倒数第几位
		digitNum = Integer.parseInt(appConf.getParam("digit_num"));//测试几位
		testData = appConf.getParam("test_data");//测试的数据
		allPhoneNumber = appConf.getParam("phone_number");
		String[] split = allPhoneNumber.split(",");
		for (int i = 0; i < split.length; i++) {
			phoneNumber.add(split[i]);
		}
		// service related configuration
		limit = new String[]{"162","163","168","169","492","493","494","495","496","407","498","499","500","501","02"};
		for(int i=0;i<limit.length;i++){
			cartoon_classid.add(limit[i]);
		}
		
	}


	/**************************************************************************************************************/
	/**************************************************************************************************************/
	/*   execute()做了以下工作：
	 *   1.拿到流里来的用户id、图书id、接口类型
	 *   2.根据classid区分阅读和动漫，然后将tuple传给不同的bolt进行处理
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
		String substring = userId.substring(userId.length()-digit, userId.length()-digit+digitNum);
	    
		if(!classId.equals("-1")){
			if(cartoon_classid.contains(classId)){
				collector.emit("cartoon-stream",new Values(userId, bookId, type, authorId, classId));
				PrintHelper.print("yueqian cartoon-stream.");
			}else{
				if(substring.equals(testData) || phoneNumber.contains(userId) ){
					if(userId.equals("15057121414")){
						collector.emit("read-stream",new Values(userId, bookId, type, authorId, classId));
						PrintHelper.print("yueqian read-stream.");
					}else{
						collector.emit("new-read-stream",new Values(userId, bookId, type , authorId, classId));
						PrintHelper.print("yueqian new-read-stream.");
					}
				}else{
					collector.emit("read-stream",new Values(userId, bookId, type, authorId, classId));
					PrintHelper.print("yueqian read-stream.");
				}
			}
				
		}
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("cartoon-stream", new Fields("user", "book", "type", "author", "classid"));
		declarer.declareStream("read-stream", new Fields("user", "book", "type", "author", "classid"));
		declarer.declareStream("new-read-stream", new Fields("user", "book", "type", "author", "classid"));
	}

}
