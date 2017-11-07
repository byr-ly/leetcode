package com.eb.bi.rs.mras.bookrec.correrecrealtimefilter;

import java.util.HashSet;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.eb.bi.rs.mras.bookrec.correrecrealtimefilter.PrintHelper;

public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private String[] limit;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		PrintHelper.print("yueqian SplitBolt prepare() begin.");
	
		// service related configuration
		limit = new String[]{"162","163","168","169","492","493","494","495","496","407","498","499","500","501","02"};
		
		PrintHelper.print("yueqian SplitBolt prepare() end.");

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

		PrintHelper.print("yueqian splitBolt execute() begin.");
		
		String userId = input.getStringByField("user");
		String bookId = input.getStringByField("book");
		String type = input.getStringByField("type");
		String authorId = input.getStringByField("author");
	    String classId = input.getStringByField("classid");
		HashSet<String> cartoon_classid = new HashSet<String>();
		for(int i=0;i<limit.length;i++){
			cartoon_classid.add(limit[i]);
		}
		if(!classId.equals("-1")){
			if(cartoon_classid.contains(classId)){
				collector.emit("cartoon-stream",new Values(userId, bookId, type, authorId, classId));
				PrintHelper.print("yueqian cartoon-stream.");
			}else{
				collector.emit("read-stream",new Values(userId, bookId, type, authorId, classId));
				PrintHelper.print("yueqian read-stream.");
			}
		}

		PrintHelper.print("yueqian splitBolt execute() end.");
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("cartoon-stream", new Fields("user", "book", "type", "author", "classid"));
		declarer.declareStream("read-stream", new Fields("user", "book", "type", "author", "classid"));
	}

}
