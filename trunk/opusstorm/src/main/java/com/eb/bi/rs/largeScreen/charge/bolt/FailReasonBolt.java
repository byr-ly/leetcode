package com.eb.bi.rs.largeScreen.charge.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

import com.eb.bi.rs.frame2.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;
import com.eb.bi.rs.largeScreen.charge.Constant;
import com.eb.bi.rs.largeScreen.charge.entity.FailReason;
import com.eb.bi.rs.largeScreen.charge.entity.FailReasonAccumulation;
import com.eb.bi.rs.largeScreen.db_oracle.DB;
import com.eb.bi.rs.largeScreen.db_oracle.DBConstant;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class FailReasonBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	//数据库
	private DB db = new DB();
	//累计交易失败原因笔数
	private String tableName;
	//输出数据间隔时间
	private long interval_time = 0;
	//刷新失败原因维表的时间间隔
	private long loadInterval;
	//输出数据延迟时间
	private long delay_time;
	// 定时器
	private transient Thread loader = null;
	//写入oracle定时器
	private transient Thread writer = null;
	//失败原因维表名
	private Object error_tableName;
	//失败原因维表数据
	private Map<String, FailReason> reason_map = new ConcurrentHashMap<String, FailReason>();
	//累计计费失败数据
	private Map<Calendar, FailReasonAccumulation> fail_data = new ConcurrentHashMap<Calendar, FailReasonAccumulation>();
	//错误代码
	private String error_code = null;
	//交易时间
	private long  trans_time = 0;
	//账期
	private long record_day = 0;
	//上一次写入oracle的交易时段
	private Calendar last_write;
	private Boolean isLock = true;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());
		tableName = appConfig.getParam("error_code_table_name");
		interval_time = Long.parseLong(appConfig.getParam("interval_time"));
		loadInterval = Long.parseLong(appConfig.getParam("load_interval"));
		error_tableName = appConfig.getParam("error_table_name");
		delay_time = Long.parseLong(appConfig.getParam("delay_time"));
		last_write= Calendar.getInstance();
		long time = System.currentTimeMillis();
		last_write.setTimeInMillis(time - time%interval_time);
		// 先Load一遍数据
		loadDataFromDB();
		
		// 定时器，每天定时从oracle读取号段和城市的映射关系
		if (loader == null) {
			loader = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(loadInterval);
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
		System.out.println("--------------------FailReasonBolt prepare end--------------------");
	}
	private void loadDataFromDB() {
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(error_tableName);
		long startTime = System.currentTimeMillis();
		try {
			String sql = "select * from " + error_tableName;
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL,DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			ResultSet rs = pst.executeQuery();
			while(rs.next()){
				String[] splits = fields.split(",");
				String error_code = rs.getString(splits[0]);
				String error_name = rs.getString(splits[1]);
				String error_src_name = rs.getString(splits[2]);
				FailReason failReason = new FailReason(error_code, error_name, error_src_name);
				reason_map.put(error_code, failReason);
			}
			con.commit();
			pst.close();
			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		PrintHelper.print("FailReasonBolt load cost " + (endTime - startTime) + "ms");
		
	}
//	private void loadLastDataFromDB() {
//		Map<String, String> parameters = db.parameters;
//		String fields = parameters.get(tableName);
//		long startTime = System.currentTimeMillis();
//		try {
//			String sql = "select * from " + tableName + " tt where record_day = to_number(to_char(sysdate, 'yyyymmdd')) and trans_date in (select max(trans_date) from tableName where record_day = to_number(to_char(sysdate, 'yyyymmdd')))";
//			Class.forName("oracle.jdbc.driver.OracleDriver");
//			Connection con = DriverManager.getConnection(DBConstant.DBURL,DBConstant.DBUSER, DBConstant.DBPASSWORD);
//			con.setAutoCommit(false);
//			PreparedStatement pst = con.prepareStatement(sql);
//			ResultSet rs = pst.executeQuery();
//			while(rs.next()){
//				String[] splits = fields.split(",");
//				long record_day = rs.getLong(splits[0]);
//				long trans_date = rs.getLong(splits[1]);
//				String error_code = rs.getString(splits[2]);
//				String error_name = rs.getString(splits[3]);
//				long fail_num = rs.getLong(splits[0]);
//				long deal_time = rs.getLong(splits[1]);
//				FailReasonAccumulation accumulation = new FailReasonAccumulation();
//				accumulation.update(trans_date, record_day);
//				accumulation.add(error_code, fail_num);
//				accumulation.setNewData(false);
//				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
//				Calendar calendar = Calendar.getInstance();
//				calendar.setTime(sdf.parse(trans_date + ""));
//				calendar.add(Calendar.MINUTE, 1);
//				fail_data.put(calendar, accumulation);
//			}
//			con.commit();
//			pst.close();
//			con.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		long endTime = System.currentTimeMillis();
//		PrintHelper.print("FailReasonBolt lost data load cost " + (endTime - startTime) + "ms");
//		
//	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 定时器，每5分钟将统计数据存入oracle
		if (writer == null) {
			writer = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							long current_time = System.currentTimeMillis();
							long sleep_time = interval_time - current_time%interval_time;
							Thread.sleep(sleep_time);
							synchronized (isLock){
								while (current_time + sleep_time - getMinTime().getTimeInMillis() >= delay_time) {
									output();								
								}
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}	
					}
				}	
			});
			writer.setDaemon(true);
			writer.start();
		}
		long begin = System.currentTimeMillis();
		error_code = input.getStringByField(Constant.ERROR_CODE);
		Calendar calendar = Calendar.getInstance();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			calendar.setTime(sdf.parse(input.getStringByField(Constant.TRANS_TIME).substring(0, 12)));
			int m = calendar.get(Calendar.MINUTE);
			calendar.add(Calendar.MINUTE, 5-m%5);
			Calendar trans_cal = (Calendar) calendar.clone();
			trans_cal.add(Calendar.MINUTE, -1);
			trans_time = Long.parseLong(sdf.format(trans_cal.getTime()));
			record_day = Long.parseLong(input.getStringByField(Constant.RECORD_DAY));
		} catch (Exception e) {
			System.out.println("TRANS_TIME : " + input.getStringByField(Constant.TRANS_TIME));
			e.printStackTrace();
			return;
		}
		FailReason failReason = reason_map.get(error_code);
		synchronized (isLock){
			if (StringUtils.isNotBlank(error_code) && failReason != null) {
				FailReasonAccumulation fail_acc = getData(calendar);
				fail_acc.update(trans_time, record_day);
				fail_acc.add(error_code, 1);
			}			
		}
		long end = System.currentTimeMillis();
//		System.out.println("**************************Execute total costs: + " + (end - begin) + "**************************");

	}
	private FailReasonAccumulation getData(Calendar calendar) {
		
		FailReasonAccumulation error_data = fail_data.get(calendar);
		if (error_data == null) {
			error_data = new FailReasonAccumulation();
			fail_data.put(calendar, error_data);
		}
		return error_data;
	}
	
	private Calendar getMinTime() {
		Calendar calendar = Calendar.getInstance();
		Set<Calendar> keySet = fail_data.keySet();
		for (Calendar cal : keySet) {
			if (cal.before(calendar) && !cal.equals(last_write)) {
				calendar = cal;
			}
		}
		return calendar;
	}
	
	//判断两组数据是否需要合并
	private boolean isMerge(Calendar last, Calendar now) {
		if (last.get(Calendar.HOUR_OF_DAY) == 0 && last.get(Calendar.MINUTE) == 0) {
			return false;
		}else if (now.get(Calendar.HOUR_OF_DAY) == 0 && now.get(Calendar.MINUTE) == 0) {
			return true;
		}else if (last.get(Calendar.DAY_OF_YEAR) == now.get(Calendar.DAY_OF_YEAR)) {
			return true;
		}else {
			return false;
		}
	}
	
	private void output() {
		Calendar calendar = getMinTime();
		FailReasonAccumulation last_data = getData(last_write);
		while (calendar.before(last_write)){
			System.out.println("fail acc min : " + calendar.getTime());
			FailReasonAccumulation old_data = getData(calendar);
			if (calendar.get(Calendar.DAY_OF_YEAR) == last_write.get(Calendar.DAY_OF_YEAR)) {
				last_data.addAll(old_data);
			}
			old_data.clear();				
			fail_data.remove(calendar);
			calendar = getMinTime();
		}
		System.out.println("fail acc last : " + last_write.getTime());
		System.out.println("fail acc cur : " + calendar.getTime());
		System.out.println("fail acc deal : " + Calendar.getInstance().getTime());
		if (calendar.getTimeInMillis() - last_write.getTimeInMillis() > 5*60*1000){
			try {
				Calendar next_cal = (Calendar) last_write.clone();
				next_cal.add(Calendar.MINUTE, 5);
				FailReasonAccumulation next = getData(next_cal);
				Calendar trans_cal = (Calendar) next_cal.clone();
				trans_cal.add(Calendar.MINUTE, -1);
				Date date = trans_cal.getTime();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				SimpleDateFormat sdf_day = new SimpleDateFormat("yyyyMMdd");
				long next_trans_date = Long.parseLong(sdf.format(date));
				long next_record_day = Long.parseLong(sdf_day.format(date));
				next.update(next_trans_date, next_record_day);
				calendar = getMinTime();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		long currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis - calendar.getTimeInMillis() >= delay_time) {
			FailReasonAccumulation fail_acc = getData(calendar);
			if (isMerge(last_write, calendar)) {
				fail_acc.addAll(last_data);				
			}
			write(fail_acc);
			last_data.clear();
			fail_data.remove(last_write);
			last_write = calendar;
		}
	}

	private void write(FailReasonAccumulation fail_acc){
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(tableName);
		long startTime = System.currentTimeMillis();
		try {
			SimpleDateFormat sdf_s = new SimpleDateFormat(Constant.TO_SS);
			long deal_time = Long.parseLong(sdf_s.format(new Date(startTime)));
			String sql = "insert into " + tableName + "(" + fields + ")" + " values(?,?,?,?,?,?)";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			long trans_date = fail_acc.getTrans_date();
			long record_day = fail_acc.getRecord_day();
			Set<String> keySet = reason_map.keySet();
			for (String key : keySet) {
				pst.setLong(1, record_day);
				pst.setLong(2, trans_date);
				pst.setString(3, key);
				pst.setString(4, reason_map.get(key).getError_name());
				pst.setLong(5, fail_acc.get(key));
				pst.setLong(6, deal_time);
				PrintHelper.print("tableName : " + tableName + "\t" + record_day + "|" + trans_date + "|" + key + "|" + reason_map.get(key).getError_name() + "|" + fail_acc.get(key) + "|" + deal_time + "|");
				pst.addBatch();
			}
			pst.executeBatch();
			con.commit();
			pst.close();
			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		PrintHelper.print("insert fail reason : " + (endTime - startTime) + "ms");			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
