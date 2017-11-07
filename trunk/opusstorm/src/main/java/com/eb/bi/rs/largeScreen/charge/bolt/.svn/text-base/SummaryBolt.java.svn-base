package com.eb.bi.rs.largeScreen.charge.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.eb.bi.rs.frame2.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;
import com.eb.bi.rs.largeScreen.charge.Constant;
import com.eb.bi.rs.largeScreen.charge.entity.ChargeSummaryData;
import com.eb.bi.rs.largeScreen.db_oracle.DB;
import com.eb.bi.rs.largeScreen.db_oracle.DBConstant;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SummaryBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	//失败类型
	private String fail_type;
	//交易成功数据表名
	private String success_tableName;
	//交易失败数据表名
	private String fail_tableName;
	//数据库
	private DB db = new DB();
	//间隔时间
	private long interval_time = 0;
	//输出数据延迟时间
	private long delay_time;
	//交易时间
	private long trans_time;
	//账期
	private long record_day;
	//写入oracle定时器
	private transient Thread writer = null;
	//计费成功数据
	private Map<Calendar, ChargeSummaryData> success_data = new ConcurrentHashMap<Calendar, ChargeSummaryData>();
	//计费失败数据
	private Map<Calendar, ChargeSummaryData> fail_data = new ConcurrentHashMap<Calendar, ChargeSummaryData>();
	//上一次写入oracle的交易时段
	private Calendar success_last_write;
	private Calendar fail_last_write;
	private Boolean isLock = true;
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		PluginConfig appConfig = ConfigReader.getInstance().initConfig(stormConf.get("AppConfig").toString());	
		fail_type = appConfig.getParam("fail_type");
		success_tableName = appConfig.getParam("success_table_name");
		fail_tableName = appConfig.getParam("fail_table_name");
		interval_time = Long.parseLong(appConfig.getParam("interval_time"));
		delay_time = Long.parseLong(appConfig.getParam("delay_time"));
		success_last_write= Calendar.getInstance();
		long time = System.currentTimeMillis();
		success_last_write.setTimeInMillis(time - time%interval_time - delay_time);
		fail_last_write = (Calendar) success_last_write.clone();
	}
	
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
							synchronized (isLock) {
								while (current_time + sleep_time - getMinTime(success_data).getTimeInMillis() >= delay_time) {
									output(success_data, success_tableName, success_last_write);
								}
								while (current_time + sleep_time - getMinTime(fail_data).getTimeInMillis() >= delay_time) {
									output(fail_data , fail_tableName, fail_last_write);								
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
		String charge_type = input.getStringByField(Constant.CHARGE_TYPE);
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
		synchronized (isLock) {
			if (fail_type.indexOf(charge_type) == -1) {
				ChargeSummaryData summaryData = getSummaryData(success_data, calendar);
				summaryData.update(1, trans_time, record_day);
			}else {
				ChargeSummaryData summaryData = getSummaryData(fail_data, calendar);
				summaryData.update(1, trans_time, record_day);
			}			
		}
		long end = System.currentTimeMillis();
//		System.out.println("**************************Execute total costs: + " + (end - begin) + "**************************");
	}
	
	private ChargeSummaryData getSummaryData(Map<Calendar, ChargeSummaryData> acc_map, Calendar calendar) {
		ChargeSummaryData summaryData = acc_map.get(calendar);
		if (summaryData == null) {
			summaryData = new ChargeSummaryData();
			acc_map.put(calendar, summaryData);
		}
		return summaryData;
	}
	
	private Calendar getMinTime(Map<Calendar, ChargeSummaryData> acc_map) {
		Calendar calendar = Calendar.getInstance();
		Set<Calendar> keySet = acc_map.keySet();
		for (Calendar cal : keySet) {
			if (cal.before(calendar)) {
				calendar = cal;
			}
		}
		return calendar;
	}

	private void output( Map<Calendar, ChargeSummaryData> acc_map, String tableName, Calendar last_write) {
		Calendar calendar = getMinTime(acc_map);
		while (!calendar.after(last_write)){
			acc_map.remove(calendar);
			calendar = getMinTime(acc_map);				
		}
		if (calendar.getTimeInMillis() - last_write.getTimeInMillis() > 5*60*1000){
			try {
				Calendar next_cal = (Calendar) last_write.clone();
				next_cal.add(Calendar.MINUTE, 5);
				ChargeSummaryData next = getSummaryData(acc_map, next_cal);
				Calendar trans_cal = (Calendar) next_cal.clone();
				trans_cal.add(Calendar.MINUTE, -1);
				Date date = trans_cal.getTime();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				SimpleDateFormat sdf_day = new SimpleDateFormat("yyyyMMdd");
				long next_trans_date = Long.parseLong(sdf.format(date));
				long next_record_day = Long.parseLong(sdf_day.format(date));
				next.update(0,next_trans_date, next_record_day);
				calendar = getMinTime(acc_map);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		long currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis - calendar.getTimeInMillis() >= delay_time) {
			ChargeSummaryData chargeSummaryData = acc_map.get(calendar);
			write(chargeSummaryData, tableName);
			last_write.setTime(calendar.getTime());
			chargeSummaryData.clear();
			acc_map.remove(calendar);
		}
	}	

	private void write(ChargeSummaryData data , String tableName){
		long current_time = System.currentTimeMillis();
		SimpleDateFormat sdf_s = new SimpleDateFormat(Constant.TO_SS);
		long deal_time = Long.parseLong(sdf_s.format(new Date(current_time)));
		data.setDeal_time(deal_time);
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(tableName);
		long startTime = System.currentTimeMillis();
		try {
			String sql = "insert into " + tableName + "(" + fields + ")" + " values(?,?,?,?)";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL,
					DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			pst.setLong(1, data.getRecord_day());
			pst.setLong(2, data.getTrans_date());
			pst.setLong(3, data.getTrans_cnt());
			pst.setLong(4, data.getDeal_time());
			PrintHelper.print("tableName : " + tableName + "\t" + data.getRecord_day() + "|" + data.getTrans_date() + "|" + data.getTrans_cnt() + "|" + data.getDeal_time() + "|");
			pst.executeUpdate();
			con.commit();
			pst.close();
			con.close();
			data.clear();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
		PrintHelper.print("summary : " + (endTime - startTime) + "ms");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
