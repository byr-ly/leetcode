package com.eb.bi.rs.frame.service.msgdispatch;

import org.apache.log4j.Logger;

import com.eb.bi.rs.frame.common.pluginutil.PluginUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A minimal Jersey Resource. Note that this class is a POJO - it does not need
 * to know anything about Guice. Although, if we wanted to Inject members using
 * Guice, we could!
 */

@Path("/")
public class SampleResource {
    private static Logger logger = PluginUtil.getInstance().getLogger(); 
  

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    @Path("{url:.*}")

    public JsonResponse procUrl(@Context HttpServletRequest request) {
    	
        String requestURI = request.getRequestURI();
        String queryString = request.getQueryString();
        //logger.info(requestURI);
        //logger.info(queryString);

        JedisPool pool = null;
        Jedis jedis = null;
        boolean borrowOrOprSuccess = true;
        try {
            pool = RedisPoolAPI.getPool();
            jedis = pool.getResource();

            List<String> values = jedis.lrange("rs_request_scene_map:" + requestURI, 0, -1);
            Iterator<String> iter = values.iterator();
            while(iter.hasNext()){            	
            	String[] fields = iter.next().split(":");
            	if(fields[2].equals("1")){
            		String ids = fields[0] + ":" + fields[1];
            		logger.info( requestURI + ":"  + ids + " has been get from redis");
                	jedis.lpush("re_request_queue:" + ids, queryString);
                	logger.info("re_request_queue:" + ids + ":" + request.getQueryString() + " has been put into redis");            		
            	}            	
            }             
            
        } catch (JedisConnectionException e) {
            borrowOrOprSuccess = false;
            if (jedis != null)
                pool.returnBrokenResource(jedis);

        } finally {
            if (borrowOrOprSuccess)
               RedisPoolAPI .returnResource(pool,jedis);
        }
        
        JsonResponse response = new JsonResponse("200","请求成功");
        return response;
    }
}


