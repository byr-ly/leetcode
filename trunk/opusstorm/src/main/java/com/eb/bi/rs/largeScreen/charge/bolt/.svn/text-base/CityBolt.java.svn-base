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
import com.eb.bi.rs.largeScreen.charge.entity.AccumulationForCity;
import com.eb.bi.rs.largeScreen.charge.entity.City;
import com.eb.bi.rs.largeScreen.db_oracle.DB;
import com.eb.bi.rs.largeScreen.db_oracle.DBConstant;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CityBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	//数据库
	private DB db = new DB();
	//交易成功数据表名
	private String success_tableName;
	//交易失败数据表名
	private String fail_tableName;
	//间隔时间
	private long interval_time;
	//输出数据延迟时间
	private long delay_time;
	//写入oracle定时器
	private transient Thread writer = null;
	//省份地市维表<city_id , city>
	private Map<String, City> cityMap = new ConcurrentHashMap<String, City>();
	//实时计费成功笔数
	private Map<Calendar, AccumulationForCity> success_data = new ConcurrentHashMap<Calendar, AccumulationForCity>();
	//实时计费失败笔数
	private Map<Calendar, AccumulationForCity> fail_data = new ConcurrentHashMap<Calendar, AccumulationForCity>();
	//失败类型
	private String fail_type;
	//收费类型
	private String charge_type;
	//城市id
	private String city_id;
	//城市名称
	private String city_name;
	//省份id
	private String prv_id;
	//省份名称
	private String prv_name;
	//交易时间
	private long trans_time;
	//账期
	private long record_day;
	//交易金额
	private double trans_chrg;
	//用户id
	private String msisdn;
	//上一次计费成功数据写入oracle的交易时段
	Calendar last_write_success;
	//上一次计费失败数据写入oracle的交易时段
	Calendar last_write_fail;
	private Boolean isLock = true;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());
		fail_type = appConfig.getParam("fail_type");
		success_tableName = appConfig.getParam("city_success_table_name");
		fail_tableName = appConfig.getParam("city_fail_table_name");
		interval_time = Long.parseLong(appConfig.getParam("interval_time"));
		delay_time = Long.parseLong(appConfig.getParam("delay_time"));
		last_write_success = Calendar.getInstance();
		long time = System.currentTimeMillis();
		last_write_success.setTimeInMillis(time - time%interval_time);
		//上一次计费失败数据写入oracle的交易时段
		last_write_fail = (Calendar) last_write_success.clone();
		System.out.println("--------------------CityBolt prepare end--------------------");
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 定时器，每5分钟将统计数据存入oracle
		if (writer == null) {
			writer = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						long current_time = System.currentTimeMillis();
						long sleep_time = interval_time - current_time%interval_time;
						try {
							Thread.sleep(sleep_time);
							synchronized (isLock) {
								while (current_time + sleep_time - getMinTime(success_data, last_write_success).getTimeInMillis() >= delay_time) {
									outPut(success_data, success_tableName, last_write_success);
								}
								while (current_time + sleep_time - getMinTime(fail_data, last_write_fail).getTimeInMillis() >= delay_time) {
									outPut(fail_data, fail_tableName, last_write_fail);							
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
		charge_type = input.getStringByField(Constant.CHARGE_TYPE);
		trans_chrg = Double.parseDouble(input.getStringByField(Constant.PRICE));
		msisdn = input.getStringByField(Constant.MSISDN);
		city_id = input.getStringByField(Constant.CITY_ID);
		city_name = input.getStringByField(Constant.CITY_NAME);
		prv_id = input.getStringByField(Constant.PRV_ID);
		prv_name = input.getStringByField(Constant.PRV_NAME);
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
		City city = cityMap.get(city_id);
		if (city == null) {
			city = new City();
		}
		city.initialize(prv_id, prv_name, city_id, city_name);
		cityMap.put(city_id, city);
		synchronized (isLock) {
			if (fail_type.indexOf(charge_type) == -1) {
				AccumulationForCity successCityData = getCityData(success_data, calendar);
				successCityData.add(city_id, trans_chrg, 1, msisdn);
				successCityData.update(trans_time, record_day);
			}else {
				AccumulationForCity failCityData = getCityData(fail_data, calendar);
				failCityData.add(city_id, trans_chrg, 1, msisdn);
				failCityData.update(trans_time, record_day);
			}
		}
		long end = System.currentTimeMillis();
//		System.out.println("**************************Execute total costs: + " + (end - begin) + "**************************");
	}

	private AccumulationForCity getCityData(Map<Calendar, AccumulationForCity> acc_map, Calendar calendar) {
		
		AccumulationForCity acc_city = acc_map.get(calendar);
		if (acc_city == null) {
			acc_city = new AccumulationForCity();
			acc_map.put(calendar, acc_city);
		}
		return acc_city;
	}

	private Calendar getMinTime(Map<Calendar, AccumulationForCity> acc_map, Calendar last) {
		Calendar calendar = Calendar.getInstance();
		Set<Calendar> keySet = acc_map.keySet();
		for (Calendar cal : keySet) {
			if (cal.before(calendar) && !cal.equals(last)) {
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

	private void outPut(Map<Calendar, AccumulationForCity> acc_map, String tableName, Calendar last_write) {
		AccumulationForCity cityData_last = getCityData(acc_map, last_write);
		Calendar calendar = getMinTime(acc_map, last_write);
		while (calendar.before(last_write)){
			AccumulationForCity cityData_old = getCityData(acc_map, calendar);
			if (calendar.get(Calendar.DAY_OF_YEAR) == last_write.get(Calendar.DAY_OF_YEAR)) {
				cityData_last.addAll(cityData_old);
			}
			cityData_old.clear();
			acc_map.remove(calendar);
			calendar = getMinTime(acc_map, last_write);
		}
		if (calendar.getTimeInMillis() - last_write.getTimeInMillis() > 5*60*1000){
			try {
				Calendar next_cal = (Calendar) last_write.clone();
				next_cal.add(Calendar.MINUTE, 5);
				AccumulationForCity next = getCityData(acc_map, next_cal);
				Calendar trans_cal = (Calendar) next_cal.clone();
				trans_cal.add(Calendar.MINUTE, -1);
				Date date = trans_cal.getTime();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
				SimpleDateFormat sdf_day = new SimpleDateFormat("yyyyMMdd");
				long next_trans_date = Long.parseLong(sdf.format(date));
				long next_record_day = Long.parseLong(sdf_day.format(date));
				next.update(next_trans_date, next_record_day);
				calendar = getMinTime(acc_map, last_write);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		long currentTimeMillis = System.currentTimeMillis();
		if (currentTimeMillis - calendar.getTimeInMillis() >= delay_time) {
			AccumulationForCity acc_city = getCityData(acc_map, calendar);
			if (isMerge(last_write, calendar)) {
				acc_city.addAll(cityData_last);				
			}
			if (acc_city.isNewData()) {
				write(acc_city, tableName);				
			}
			cityData_last.clear();
			acc_map.remove(last_write);
			last_write.setTime(calendar.getTime());
		}
	}
	
	private void write(AccumulationForCity data, String tableName){
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(tableName);
		long startTime = System.currentTimeMillis();
		try {
			String sql = "insert into " + tableName + "(" + fields + ")"
					+ " values(?,?,?,?,?,?,?,?,?,?)";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL,DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			long record_day = data.getRecord_day();
			long trans_date = data.getTrans_date();
			Map<String, Double> charge_map = data.getCity_charge();
			Map<String, Long> count_map = data.getCity_count();
			Map<String, Set<String>> uv_map = data.getCity_uv();
			SimpleDateFormat sdf_s = new SimpleDateFormat(Constant.TO_SS);
			long deal_time = Long.parseLong(sdf_s.format(new Date(startTime)));
			Set<String> keySet = charge_map.keySet();
			for (String key : keySet) {
				pst.setLong(1, record_day);
				pst.setLong(2, trans_date);
				pst.setString(3, cityMap.get(key).getPrv_id());
				pst.setString(4, cityMap.get(key).getPrv_name());
				pst.setString(5, cityMap.get(key).getCity_id());
				pst.setString(6, cityMap.get(key).getCity_name());
				pst.setLong(7, count_map.get(key));
				pst.setLong(8, uv_map.get(key).size());
				pst.setLong(9, charge_map.get(key).longValue());
				pst.setLong(10, deal_time);
				PrintHelper.print("tableName : " + tableName + "\t" +  record_day + "|" + trans_date + "|" + cityMap.get(key).getPrv_id() + "|" + cityMap.get(key).getPrv_name() + "|" + cityMap.get(key).getCity_id() + "|" + cityMap.get(key).getCity_name() + "|" + count_map.get(key) + "|" + uv_map.get(key).size() + "|" + charge_map.get(key).longValue() + "|" + deal_time + "|");
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
		PrintHelper.print("insert city : " + (endTime - startTime) + "ms");			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
