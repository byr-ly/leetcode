package com.eb.bi.rs.mrasstorm.unifyrec.subversionstorm.domain;

/**
 * @author ynn 
 * @date 创建时间：2015-11-30 下午3:41:46
 * @version 1.0
 */
public class BookSeriesInfo {
	
	private String bookId;
	private float score;
	private String clazz;
	private int seriesId;
	private int orderId;
	
	public BookSeriesInfo(String bookId, float score, String clazz, int seriesId, int orderId){
		this.bookId = bookId;
		this.score = score;
		this.clazz = clazz;
		this.seriesId = seriesId;
		this.orderId = orderId;
	}
	
	public BookSeriesInfo(String bookId, int seriesId, int orderId){
		this.bookId = bookId;
		this.seriesId = seriesId;
		this.orderId = orderId;
	}
	
	public BookSeriesInfo(String bookId, int seriesId, int orderId, float score){
		this.bookId = bookId;
		this.seriesId = seriesId;
		this.orderId = orderId;
		this.score = score;
	}
	
	public String getBookId() {
		return bookId;
	}
	public void setBookId(String bookId) {
		this.bookId = bookId;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public String getClazz() {
		return clazz;
	}
	public void setClazz(String clazz) {
		this.clazz = clazz;
	}
	public int getSeriesId() {
		return seriesId;
	}
	public void setSeriesId(int seriesId) {
		this.seriesId = seriesId;
	}
	public int getOrderId() {
		return orderId;
	}
	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}
	
}
