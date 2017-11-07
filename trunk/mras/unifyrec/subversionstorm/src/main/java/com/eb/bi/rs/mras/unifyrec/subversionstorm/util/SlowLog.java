package com.eb.bi.rs.mras.unifyrec.subversionstorm.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/*
 * 打印指定用户的详细日志
 */
public class SlowLog {

	private static final SimpleDateFormat sf = new SimpleDateFormat("mm:ss:SSS");
	private static final Logger log = Logger.getLogger(SlowLog.class);

	private List<String> userList = new ArrayList<String>();

	public SlowLog(String userIds) {
		userList.clear();
		String users[] = userIds.split("&");
		for (String userid : users) {
			userList.add(userid);
		}
	}

	public void printSimilarScore(String userid,
			Map<String, Float> similarScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] SimilarScoreMap : " + similarScoreMap);
		}
	}

	public void printPrefScore(String userid, Map<String, Float> prefScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] PrefScoreMap : " + prefScoreMap);
		}
	}
	
	public void printRealBooks(String userid, Set<String> realBooks) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] Real Books : " + realBooks);
		}
	}
	
	public void printGreyBooks(String userid, Set<String> greyBooks) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] Grey Books : " + greyBooks);
		}
	}
	
	public void printBlackBooks(String userid, Set<String> blackBooks) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] Black Books : " + blackBooks);
		}
	}

	public void printHisBlackBooks(String userId, Set<String> hisBlackBooks) {
		if (userList.contains(userId)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userId
					+ " ] History Black Books : " + hisBlackBooks);
		}
	}
	
	public void printAfterMixCal(String userid,
			Map<String, Float> bookScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] AfterMixCal, BookScoreMap : " + bookScoreMap);
		}
	}
	
	public void printAfterFilterSeriesBooks(String userid,
			Map<String, Float> bookScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] AfterFilterSeriesBooks, BookScoreMap : " + bookScoreMap);
		}
	}
	
	public void printAfterFilterSameAuthor(String userid,
			Map<String, Float> bookScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] AfterFilterSameAuthor, BookScoreMap : " + bookScoreMap);
		}
	}
	
	public void printAfterTopN(String userid,
			Map<String, Float> bookScoreMap) {
		if (userList.contains(userid)) {
			log.info(sf.format(new Date()) + " [ Print Slow Logs " + userid
					+ " ] AfterTopN, BookScoreMap : " + bookScoreMap);
		}
	}

	public List<String> getUserList() {
		return userList;
	}

	public void setUserList(List<String> userList) {
		this.userList = userList;
	}


}
