package com.eb.bi.rs.mras.andnewsrec.getRetsAndFilter;

/**
 * Created by LiMingji on 2016/3/21.
 * <p/>
 * 存储详细新闻和相似得分，用于TreeMap取TopN
 */
public class CompNews implements Comparable<CompNews> {

    public String newsID;
    public Double scores;

    public CompNews(String newsID, Double scores) {
        this.newsID = newsID;
        this.scores = scores;
    }

    public CompNews(String newsID, String scoresStr1, String scoresStr2) {
        this.newsID = newsID;
        Double score1 = Double.parseDouble(scoresStr1);
        Double score2 = Double.parseDouble(scoresStr2);
        this.scores = score1 * score2;
    }

    @Override
    public int hashCode() {
        return newsID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        CompNews objt = (CompNews) obj;
        return this.newsID.equals(objt.newsID);
    }

    @Override
    public int compareTo(CompNews o) {
        if (this.newsID.equals(o.newsID)) {
            return 0;
        } else {
            int ret = this.scores.compareTo(o.scores);
            return ret > 0 ? -1 : 1;
        }
    }

    @Override
    public String toString() {
        return newsID + "|" + scores;
    }
}
