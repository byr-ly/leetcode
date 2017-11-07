package com.eb.bi.rs.mras2.cartoonrec.corelationrec.selfSign;

public class BookInfo {

	private String id;
	private String score;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getScore() {
		return score;
	}
	public void setScore(String score) {
		this.score = score;
	}
	public BookInfo() {
		super();
		// TODO Auto-generated constructor stub
	}
	public BookInfo(String id, String score) {
		super();
		this.id = id;
		this.score = score;
	}
	@Override
	public String toString() {
		return "BookInfo [id=" + id + ", score=" + score + "]";
	}
	
}
