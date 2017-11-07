package com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class PrintHelper {

	private static final SimpleDateFormat sf = new SimpleDateFormat("mm:ss:SSS");
	private static final Logger log = Logger.getLogger(PrintHelper.class);
	
	public static void print(String out) {
		log.info(sf.format(new Date()) + " [" + Thread.currentThread().getName() + "] " + out);
	}	
}
