package com.eb.bi.rs.largeScreen.charge.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.eb.bi.rs.frame2.common.storm.config.ConfigReader;
import com.eb.bi.rs.frame2.common.storm.config.PluginConfig;
import com.eb.bi.rs.largeScreen.charge.Constant;
import com.eb.bi.rs.largeScreen.charge.entity.City;
import com.eb.bi.rs.largeScreen.charge.entity.FailReasonAccumulation;
import com.eb.bi.rs.largeScreen.db_oracle.DB;
import com.eb.bi.rs.largeScreen.db_oracle.DBConstant;
import com.eb.bi.rs.opusstorm.util.PrintHelper;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	//加载数据定时器
	private transient Thread loader = null;
	//刷新号段省份地市维表的时间间隔
	private long loadInterval;
	//数据库
	private DB db = new DB();
	//号段省份地市维表名
	private String city_tableName;
	//号段省份地市维表名
	private String list_tableName;
	//号段省份地市维表映射
	private Map<String, City> cityMap = new ConcurrentHashMap<String, City>();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		PluginConfig appConfig = ConfigReader.getInstance().initConfig(
				stormConf.get("AppConfig").toString());
		loadInterval = Long.parseLong(appConfig.getParam("load_interval"));
		city_tableName = appConfig.getParam("city_table_name");
		list_tableName = appConfig.getParam("list_table_name");
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
		System.out.println("--------------------SplitBolt prepare end--------------------");
	}
	private void loadDataFromDB() {
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(city_tableName);
		long startTime = System.currentTimeMillis();
		try {
			String sql = "select * from " + city_tableName;
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL,
					DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			ResultSet rs = pst.executeQuery();
			while(rs.next()){
				String[] splits = fields.split(",");
				String msisdn_part = rs.getString(splits[0]);
				String prv_id = rs.getString(splits[1]);
				String prv_name = rs.getString(splits[2]);
				String city_id = rs.getString(splits[3]);
				String city_name = rs.getString(splits[4]);
				City city = cityMap.get(msisdn_part);
				if (city == null) {
					city = new City();
				}
				city.initialize(prv_id, prv_name, city_id, city_name);
				cityMap.put(msisdn_part, city);
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
		PrintHelper.print("splitBolt load cost " + (endTime - startTime) + "ms");
		
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String data = input.getString(0);
		String[] splits = data.split("\\|");
		String msisdn = splits[2];
		String trans_time = splits[3];
		String price = splits[16];
		String record_day = splits[18];
		String charge_type = splits[25];
		String error_code = splits[26];
		try {
			String msisdn_part = msisdn.substring(0, 7);
			City city = cityMap.get(msisdn_part);
			String city_id = city.getCity_id();
			String city_name = city.getCity_name();
			String prv_id = city.getPrv_id();
			String prv_name = city.getPrv_name();
			collector.emit(new Values(city_id, city_name, prv_id, prv_name, msisdn, trans_time, price , record_day , charge_type , error_code));
			PrintHelper.print("message : " + city_id + "|" + city_name + "|" + prv_id + "|" + prv_name + "|" + msisdn + "|" + trans_time + "|" + price + "|" + record_day + "|" + charge_type + "|" + error_code);	
			write(city_id, city_name, prv_id, prv_name, msisdn, trans_time, price , record_day , charge_type , error_code);	
		} catch (Exception e) {
			System.out.println("error : msisdn " + msisdn + "is not a phone number or it is belong to no city");
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constant.CITY_ID, Constant.CITY_NAME, Constant.PRV_ID, Constant.PRV_NAME, Constant.MSISDN, Constant.TRANS_TIME, Constant.PRICE, Constant.RECORD_DAY, Constant.CHARGE_TYPE, Constant.ERROR_CODE));
	}
	
	private void write(String city_id, String city_name, String prv_id, String prv_name, String msisdn, String trans_time, String price, String record_day2, String charge_type, String error_code){
		String fields = "RECORD_DAY,PRV_ID,PRV_NAME,CITY_ID,CITY_NAME,TRANS_TIME,MSISDN,PRICE,CHARGE_TYPE,ERROR_CODE,RECEIVE_TIME";
		long startTime = System.currentTimeMillis();
		try {
			SimpleDateFormat sdf_s = new SimpleDateFormat(Constant.TO_SS);
			String deal_time = sdf_s.format(new Date(startTime));
			String sql = "insert into " + list_tableName + "(" + fields + ")" + " values(?,?,?,?,?,?,?,?,?,?,?)";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL, DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			pst.setString(1, record_day2);
			pst.setString(2, prv_id);
			pst.setString(3, prv_name);
			pst.setString(4, city_id);
			pst.setString(5, city_name);
			pst.setString(6, trans_time);
			pst.setString(7, msisdn);
			pst.setString(8, price);
			pst.setString(9, charge_type);
			pst.setString(10, error_code);
			pst.setString(11, deal_time);
			pst.addBatch();
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
	}
}
