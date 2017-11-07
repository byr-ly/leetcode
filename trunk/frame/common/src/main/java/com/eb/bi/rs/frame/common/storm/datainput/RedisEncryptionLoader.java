package com.eb.bi.rs.frame.common.storm.datainput;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author ynn
 * @desc 访问Redis加密
 */
public class RedisEncryptionLoader extends LoaderBase {
	private Jedis jedis_init = null;
	private String [] addrs;
	
	// 格式：IP::Port::PassWord::Key
	protected boolean connectInit() {
		addrs =  m_confData.getLink().split("::");
		jedis_init = new Jedis(addrs[0], Integer.parseInt(addrs[1]));	
		jedis_init.auth(addrs[2]);
		return true;
	}
	
	@Override	
	public DataRecord getRecord() {
		String record_str = null;
		try {
			if(this.m_confData.getLoadCommand().equals("rpop")){
				record_str = jedis_init.rpop(addrs[3]);
			}else if(this.m_confData.getLoadCommand().equals("lpop")) {
				record_str = jedis_init.lpop(addrs[3]);
			}
		} catch (JedisConnectionException e1) {
			System.out.println("JedisConnectionException[pop] is catched");
			e1.printStackTrace();			
			if (!jedis_init.isConnected()) {
				try {
					jedis_init.connect();
				} catch (JedisConnectionException e2) {
					System.out.println("JedisConnectionException[connect] is catched");
					e2.printStackTrace();
				} 
			}
			return null;//connection异常后直接返回null。
		} 
		
		if(record_str == null)
			return null;
		
		DataRecord dataRecord = new DataRecord(m_parser.m_meta);
		if(!m_parser.doParse(record_str, dataRecord)){
			System.out.println("invalid record!");
			return null;
		}
		return dataRecord;
	}
	
}
