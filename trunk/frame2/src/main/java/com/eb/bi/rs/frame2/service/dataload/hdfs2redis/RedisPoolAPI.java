package com.eb.bi.rs.frame2.service.dataload.hdfs2redis;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * Redis操作接口 *
 */
public class RedisPoolAPI {

    private static JedisPool pool = null;

    /**
     * 创建连接池 *
     *
     * @return pool
     */
    public static JedisPool getPool() {

        if (pool == null) {
//        	 Properties prop = new Properties();
//             try {
//             	prop.load(new FileInputStream(PluginUtil.getInstance().getConfig().getConfigFilePath()));		
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }


            JedisPoolConfig config = new JedisPoolConfig();
//             config.setMaxActive(Integer.valueOf(prop.getProperty("redis.pool.maxActive")));
//             config.setMaxIdle(Integer.valueOf(prop.getProperty("redis.pool.maxIdle")));
//             config.setMaxWait(Long.valueOf(prop.getProperty("redis.pool.maxWait")));
//             config.setTestOnBorrow(Boolean.valueOf(prop.getProperty("redis.pool.testOnBorrow")));
//             config.setTestOnReturn(Boolean.valueOf(prop.getProperty("redis.pool.testOnReturn")));
            // pool = new JedisPool(config, prop.getProperty("redis.ip"),Integer.valueOf(prop.getProperty("redis.port"))); 

            pool = new JedisPool(config, "10.1.70.4", 3033);


//            ResourceBundle bundle = ResourceBundle.getBundle("redis");
//            if (bundle == null) {
//                throw new IllegalArgumentException(
//                        "[redis.properties] is not found!");
//            }
//            JedisPoolConfig config = new JedisPoolConfig();
//            config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
//            config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
//            config.setMaxWait(Long.valueOf(bundle.getString("redis.pool.maxWait")));
//            config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
//            config.setTestOnReturn(Boolean.valueOf(bundle .getString("redis.pool.testOnReturn")));
//            pool = new JedisPool(config, bundle.getString("redis.ip"), Integer.valueOf(bundle.getString("redis.port")));

//        	PluginUtil pluginUtil = PluginUtil.getInstance();
//        	pluginUtil.init(args);
//        	Logger log = pluginUtil.getLogger();
            //String configFilePath = pluginUtil.getConfig().getConfigFilePath();


        }
        return pool;
    }

    /**
     * 返还到连接池     *
     *
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        if (redis != null) {
            pool.returnResource(redis);
        }
    }
}




