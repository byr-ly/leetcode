package com.eb.bi.rs.frame.common.hbase.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.*;

public class HbaseScannerByTime{

	public static List<Result> getFillerBook(HTableInterface table, 
    		long minRecords, int tryTimes) {

    	List<Result> resultList = new ArrayList<Result>();
        Scan scan = new Scan();
        scan.setCaching(100);
        try {
        	long maxStamp = System.currentTimeMillis();//获取数据的结束时间
			long minStamp = maxStamp - 24 * 60 * 60 * 1000;//获取数据的起始时间
			scan.setTimeRange(minStamp, maxStamp);
			int count = 0;//获取次数
			while (resultList.size() < minRecords && count < tryTimes) {
				count++;
				ResultScanner rs = table.getScanner(scan);
	            for (Result result : rs) {
	            	resultList.add(result);
	            }
				minStamp -= 24 * 60 * 60 * 1000;
				maxStamp -= 24 * 60 * 60 * 1000;
			}
			//获取tryTimes次后补图书扔少于minRecords本，取全表数据。
			if(resultList.size() < minRecords){
				scan.setTimeRange(0, maxStamp);
				ResultScanner rs = table.getScanner(scan);
	            for (Result result : rs) {
	            	resultList.add(result);
	            }
			}
			
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultList;
    }
}

