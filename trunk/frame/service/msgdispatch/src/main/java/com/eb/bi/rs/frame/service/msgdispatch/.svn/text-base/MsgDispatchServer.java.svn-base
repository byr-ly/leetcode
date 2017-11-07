package com.eb.bi.rs.frame.service.msgdispatch;



import java.io.FileInputStream;
import java.util.EnumSet;
import java.util.Properties;

import javax.servlet.DispatcherType;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import  com.eb.bi.rs.frame.common.pluginutil.PluginConfig;
import  com.eb.bi.rs.frame.common.pluginutil.PluginExitCode;
import  com.eb.bi.rs.frame.common.pluginutil.PluginResult;
import  com.eb.bi.rs.frame.common.pluginutil.PluginUtil;
import com.google.inject.servlet.GuiceFilter;


/**
 * Starts an embedded Jetty server.
 */
public class MsgDispatchServer {


    public static void main(String[] args) throws Exception {
    	
    	PluginUtil pluginUtil = PluginUtil.getInstance();
    	pluginUtil.init(args);
    	Logger log = pluginUtil.getLogger();
    	String configFilePath = pluginUtil.getConfig().getConfigFilePath();
    	
    	
    	Properties prop = new Properties();
    	try {
			prop.load(new FileInputStream(configFilePath));			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	
    	
    	
    	//log4j，
//        Properties props = new Properties(); 
//        try{        
//	        props.load(MsgDispatchServer.class.getClassLoader().getResourceAsStream("log4j.properties"));
//	    	PropertyConfigurator.configure(props); 
//        } catch (IOException e) {
//    		e.printStackTrace();
//    	}
    	
    	
    	//jetty，
//    	try {
//    		props.load(MsgDispatchServer.class.getClassLoader().getResourceAsStream("jetty.properties"));
//    	} catch (IOException e) {
//    		e.printStackTrace();
//    	}
    	
        Server server = new Server(Integer.parseInt(prop.getProperty("jetty.port")));
        ServletContextHandler root = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);

        root.addEventListener(new SampleConfig());
        root.addFilter(GuiceFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
        root.addServlet(EmptyServlet.class, "/*");

        server.start();
        log.info("the jetty server start up");
    }
}
