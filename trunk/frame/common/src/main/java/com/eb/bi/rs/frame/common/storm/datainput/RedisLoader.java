package com.eb.bi.rs.frame.common.storm.datainput;



import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisLoader extends LoaderBase {
	private Jedis jedis_init = null;
	private String [] addrs;
	
	protected boolean connectInit() {
		addrs =  m_confData.getLink().split("::");
		jedis_init = new Jedis(addrs[0], Integer.parseInt(addrs[1]));		
		return true;
	}
	
	
	@Override	
	public DataRecord getRecord() {
		String record_str = null;
		try {
			if(this.m_confData.getLoadCommand().equals("rpop")){
				record_str = jedis_init.rpop(addrs[2]);
			}else if(this.m_confData.getLoadCommand().equals("lpop")) {
				record_str = jedis_init.lpop(addrs[2]);
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

	private void linkmanager(String addr){
		
	}
	
}
