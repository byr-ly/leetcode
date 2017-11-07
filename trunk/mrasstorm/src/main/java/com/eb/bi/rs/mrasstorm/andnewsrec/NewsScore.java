package com.eb.bi.rs.mrasstorm.andnewsrec;

public class NewsScore implements Comparable<NewsScore> {

	String news; 
	float score;
	public NewsScore(String news, float score) {
		this.news = news;
		this.score = score;
	}

	public String getNews() {
		return news;
	}

	public void setNews(String news) {
		this.news = news;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	@Override
	public int compareTo(NewsScore newsScore) {
		if(newsScore.getScore() == score){
			return 0;
		} else if (newsScore.getScore() > score){
			return 1;
		} else {
			return -1;
		}
	}

}
